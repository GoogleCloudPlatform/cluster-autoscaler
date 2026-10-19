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

package locationpolicy

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	ca_errors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

func TestLocationPolicyProcessor(t *testing.T) {
	zones := []string{"us-test1-a", "us-test1-b", "us-test1-c"}

	defaultResponse := []nodegroupset.ScaleUpInfo{
		{Group: gke.NewTestGkeMigBuilder().Build(), CurrentSize: 0, NewSize: 5, MaxSize: 10},
		{Group: gke.NewTestGkeMigBuilder().Build(), CurrentSize: 0, NewSize: 5, MaxSize: 10},
	}
	balancerResponse := []nodegroupset.ScaleUpInfo{
		{Group: gke.NewTestGkeMigBuilder().Build(), CurrentSize: 0, NewSize: 10, MaxSize: 10},
	}
	faMig := gke.NewTestGkeMigBuilder().
		SetGceRefZone("us-test1-a").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "e2-standard-4",
			Labels:      map[string]string{labels.ComputeClassLabel: "ccc-1"},
		}).
		Build()
	faBalancerResponse := []nodegroupset.ScaleUpInfo{
		{Group: faMig, CurrentSize: 0, NewSize: 10, MaxSize: 10},
	}

	balancerCallOk := balancerCall{
		response: balancerResponse,
	}
	faBalancerCallOk := balancerCall{
		response: faBalancerResponse,
	}
	balancerCallError := balancerCall{
		err: fmt.Errorf("balancer error"),
	}
	noCapacityErr := balancerCall{
		err: &googleapi.Error{
			Code:    403,
			Message: "no capacity",
		},
	}

	testCases := []struct {
		name                           string
		getTargetSizeErr               bool
		balancers                      map[gke.LocationPolicyEnum]*balancerMock
		balancerCalls                  map[gke.LocationPolicyEnum]balancerCall
		locationPolicy                 gke.LocationPolicyEnum
		newNodes                       int
		enabledExperimentFlags         []string
		disabledExperimentFlags        []string
		isFlexAdvisorEnabled           bool
		setupFlexAdvisor               func(iaProvider *instanceavailability.MockProvider)
		npSpec                         *gkeclient.NodePoolSpec
		wantScaleUpInfo                []nodegroupset.ScaleUpInfo
		provisioningRequestScaleUpMode bool
		wantErr                        bool
	}{
		{
			name: "migs with location policy ANY, balancer OK, return balancer result",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: balancerCallOk,
			},
			locationPolicy:  gke.LocationPolicyAny,
			newNodes:        10,
			wantScaleUpInfo: balancerResponse,
		},
		{
			name: "migs with location policy ANY, balancer error, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: balancerCallError,
			},
			locationPolicy:  gke.LocationPolicyAny,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name: "migs with location policy ANY, no balancer, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyBalanced: {},
			},
			locationPolicy:  gke.LocationPolicyAny,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name: "migs with location policy ANY, experiment flag disabled balancing for Flex Start TPU",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			locationPolicy:         gke.LocationPolicyAny,
			newNodes:               10,
			enabledExperimentFlags: []string{experiments.RecommendLocationsDisabledForTPUFlag},
			npSpec:                 &gkeclient.NodePoolSpec{TpuType: "tpu-v5-lite-podslice", FlexStart: true},
			wantScaleUpInfo:        defaultResponse,
		},
		{
			name: "migs with location policy BALANCED, balancer OK, return balancer result",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyBalanced: balancerCallOk,
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        10,
			wantScaleUpInfo: balancerResponse,
		},
		{
			name: "migs with location policy BALANCED, balancer error, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyBalanced: balancerCallError,
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name: "migs with location policy BALANCED, no balancer, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny: {},
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name: "migs with location policy UNSPECIFIED, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			locationPolicy:  gke.LocationPolicyUnspecified,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name: "new nodes exceed migs capacity, return default",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        100,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name:             "mig get target size error, return default",
			getTargetSizeErr: true,
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name:                           "ProvReq scaleUp mode, no capacity error, return error",
			provisioningRequestScaleUpMode: true,
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: noCapacityErr,
			},
			locationPolicy: gke.LocationPolicyAny,
			newNodes:       10,
			wantErr:        true,
		},
		{
			name:                           "ProvReq scaleUp mode, unknown error, return default",
			provisioningRequestScaleUpMode: true,
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: balancerCallError,
			},
			locationPolicy:  gke.LocationPolicyAny,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name:                           "ProvReq scaleUp mode, location policy balanced, return default",
			provisioningRequestScaleUpMode: true,
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyBalanced: balancerCallError,
			},
			locationPolicy:  gke.LocationPolicyBalanced,
			newNodes:        10,
			wantScaleUpInfo: defaultResponse,
		},
		{
			name:                           "ProvReq scaleUp mode, location policy any, capacity found",
			provisioningRequestScaleUpMode: true,
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: balancerCallOk,
			},
			locationPolicy:  gke.LocationPolicyAny,
			newNodes:        10,
			wantScaleUpInfo: balancerResponse,
		},
		{
			name: "migs with location policy ANY, FlexAdvisor enabled and processing enabled - notifies FlexAdvisor",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: faBalancerCallOk,
			},
			locationPolicy:         gke.LocationPolicyAny,
			newNodes:               10,
			isFlexAdvisorEnabled:   true,
			enabledExperimentFlags: []string{experiments.FlexAdvisorProcessingEnabledFlag, experiments.FlexAdvisorProcessingMinCAVersionFlag},
			setupFlexAdvisor: func(iaProvider *instanceavailability.MockProvider) {
				snapshot := instanceavailability.NewSnapshot(iaProvider, "ccc-1", "", "guidance-1", "", map[string]int{"us-test1-a": 10}, map[string]float64{"us-test1-a": 1.0})
				iaProvider.On("BumpScopeTtl", "ccc-1").Return(nil).Once()
				iaProvider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(snapshot, nil).Once()
				iaProvider.On("MarkUsed", "ccc-1", "", "guidance-1", mock.Anything, map[string]int{"us-test1-a": 10}).Return(nil).Once()
			},
			wantScaleUpInfo: faBalancerResponse,
		},
		{
			name: "migs with location policy ANY, FlexAdvisor enabled and FlexAdvisorProcessing::Enabled disabled - does not notify FlexAdvisor",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: faBalancerCallOk,
			},
			locationPolicy:          gke.LocationPolicyAny,
			newNodes:                10,
			isFlexAdvisorEnabled:    true,
			disabledExperimentFlags: []string{experiments.FlexAdvisorProcessingEnabledFlag},
			setupFlexAdvisor: func(iaProvider *instanceavailability.MockProvider) {
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed should not be called")
			},
			wantScaleUpInfo: faBalancerResponse,
		},
		{
			name: "migs with location policy ANY, FlexAdvisor enabled and FlexAdvisorProcessing::MinCAVersion disabled - does not notify FlexAdvisor",
			balancers: map[gke.LocationPolicyEnum]*balancerMock{
				gke.LocationPolicyAny:      {},
				gke.LocationPolicyBalanced: {},
			},
			balancerCalls: map[gke.LocationPolicyEnum]balancerCall{
				gke.LocationPolicyAny: faBalancerCallOk,
			},
			locationPolicy:          gke.LocationPolicyAny,
			newNodes:                10,
			isFlexAdvisorEnabled:    true,
			disabledExperimentFlags: []string{experiments.FlexAdvisorProcessingMinCAVersionFlag},
			setupFlexAdvisor: func(iaProvider *instanceavailability.MockProvider) {
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed should not be called")
			},
			wantScaleUpInfo: faBalancerResponse,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager := &gke.GkeManagerMock{}
			gkeManager.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2)
			gkeManager.On("IsResizeRequestErrorHandlingEnabled").Return(true)
			gkeManager.On("ResizeRequests", mock.Anything).Return([]rrclient.ResizeRequestStatus{}, nil)

			provider, err := gke.BuildGkeCloudProvider(gkeManager, nil, nil, true, "us-test1", nil, false, false, nil, "", nil, nil, nil, 1000)
			if err != nil {
				t.Fatalf("gke.BuildGkeCloudProvider() error = %v", err)
			}

			var nodeGroups []cloudprovider.NodeGroup
			var gkeNodeGroups []gke.NodeGroup
			for _, zone := range zones {
				config := testMigConfig{"test", zone, fmt.Sprintf("mig-%s", tc.locationPolicy), 0, 10, "link", tc.locationPolicy, tc.npSpec}
				mig := newTestMig(gkeManager, config, false, tc.getTargetSizeErr)
				nodeGroups = append(nodeGroups, mig)
				gkeNodeGroups = append(gkeNodeGroups, mig)
			}

			mockProcessor := &nodeGroupSetProcessorMock{}
			mockProcessor.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, nodeGroups, tc.newNodes).Return(defaultResponse, nil).Once()

			balancers := make(map[gke.LocationPolicyEnum]Balancer)
			for policy, balancer := range tc.balancers {
				call, found := tc.balancerCalls[policy]
				if found {
					balancer.On("Balance", gkeNodeGroups, tc.newNodes).Return(call.response, call.err).Once()
				}
				balancers[policy] = balancer
			}

			boolFlags := make(map[string]bool)
			for _, f := range tc.enabledExperimentFlags {
				boolFlags[f] = true
			}
			for _, f := range tc.disabledExperimentFlags {
				boolFlags[f] = false
			}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, nil)

			var iaProvider *instanceavailability.MockProvider
			var cccLister lister.Lister
			if tc.isFlexAdvisorEnabled {
				iaProvider = new(instanceavailability.MockProvider)
				iaProvider.On("IncrementFlexAdvisorCacheQueryCount", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()
				if tc.setupFlexAdvisor != nil {
					tc.setupFlexAdvisor(iaProvider)
				}
				cccLister = lister.NewMockCrdListerWithLabel([]crd.CRD{
					ccc.NewCccCrd(&v1.ComputeClass{
						ObjectMeta: metav1.ObjectMeta{Name: "ccc-1"},
					}, "", false, nil, nil),
				}, labels.ComputeClassLabel)
			}

			processor := NewProcessor(mockProcessor, provider, balancers, experimentsManager, tc.isFlexAdvisorEnabled, iaProvider, cccLister)
			scaleUpInfo, err := processor.BalanceScaleUpBetweenGroups(context.TODO(), &ca_context.AutoscalingContext{ProvisioningRequestScaleUpMode: tc.provisioningRequestScaleUpMode}, nodeGroups, tc.newNodes)

			for _, balancer := range tc.balancers {
				balancer.AssertExpectations(t)
			}
			if iaProvider != nil {
				iaProvider.AssertExpectations(t)
			}

			if (err != nil) != tc.wantErr {
				t.Errorf("Processor.BalanceScaleUpBetweenGroups() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if diff := cmp.Diff(tc.wantScaleUpInfo, scaleUpInfo, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Processor.BalanceScaleUpBetweenGroups() diff (-want +got):\n%s", diff)
			}
		})
	}
}

type balancerMock struct {
	mock.Mock
}

// Balance is a mocked method of the Balancer interface.
func (m *balancerMock) Balance(groups []gke.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, error) {
	args := m.Called(groups, newNodes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]nodegroupset.ScaleUpInfo), args.Error(1)
}

type balancerCall struct {
	response []nodegroupset.ScaleUpInfo
	err      error
}

type nodeGroupSetProcessorMock struct {
	mock.Mock
}

// FindSimilarNodeGroups is a mocked method of the NodeGroupSetProcessor interface.
func (m *nodeGroupSetProcessorMock) FindSimilarNodeGroups(ctx context.Context, _ *ca_context.AutoscalingContext, _ cloudprovider.NodeGroup, _ map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, ca_errors.AutoscalerError) {
	return nil, nil
}

// BalanceScaleUpBetweenGroups is a mocked method of the NodeGroupSetProcessor interface.
func (m *nodeGroupSetProcessorMock) BalanceScaleUpBetweenGroups(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, ca_errors.AutoscalerError) {
	args := m.Called(ctx, autoscalingCtx, groups, newNodes)
	var result []nodegroupset.ScaleUpInfo
	var err ca_errors.AutoscalerError

	if args.Get(0) != nil {
		result = args.Get(0).([]nodegroupset.ScaleUpInfo)
	}
	if args.Get(1) != nil {
		err = args.Get(1).(ca_errors.AutoscalerError)
	}

	return result, err
}

// CleanUp is a mocked method of the NodeGroupSetProcessor interface.
func (m *nodeGroupSetProcessorMock) CleanUp() {
}

type metricsAggregatorMock struct {
	mock.Mock
}

func singleCallMetricsAggregatorMock(locationPolicy gke.LocationPolicyEnum, status string) *metricsAggregatorMock {
	aggregator := &metricsAggregatorMock{}
	aggregator.On("ProcessStatus", locationPolicy, status).Once()
	return aggregator
}

func (m *metricsAggregatorMock) ProcessStatus(locationPolicy gke.LocationPolicyEnum, status string) {
	m.Called(locationPolicy, status)
}

func TestGetNodeGroupSetCapacity(t *testing.T) {
	testCases := []struct {
		name             string
		getTargetSizeErr bool
		configs          []testMigConfig
		wantCapacity     int
		wantErr          bool
	}{
		{
			name: "single mig under capacity",
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 2, 10, "link-1", gke.LocationPolicyAny, nil},
			},
			wantCapacity: 8,
		},
		{
			name: "single mig at capacity",
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 10, 10, "link-1", gke.LocationPolicyAny, nil},
			},
			wantCapacity: 0,
		},
		{
			name: "single mig over capacity",
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 20, 10, "link-1", gke.LocationPolicyAny, nil},
			},
			wantCapacity: 0,
		},
		{
			name: "multiple migs under capacity",
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 2, 10, "link-1", gke.LocationPolicyAny, nil},
				{"test", "us-test1-a", "mig-2", 5, 10, "link-2", gke.LocationPolicyAny, nil},
			},
			wantCapacity: 13,
		},
		{
			name: "multiple migs, one over capacity",
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 20, 10, "link-1", gke.LocationPolicyAny, nil},
				{"test", "us-test1-a", "mig-2", 5, 10, "link-1", gke.LocationPolicyAny, nil},
			},
			wantCapacity: 5,
		},
		{
			name:             "mig get target size error",
			getTargetSizeErr: true,
			configs: []testMigConfig{
				{"test", "us-test1-a", "mig-1", 10, 10, "link-1", gke.LocationPolicyAny, nil},
			},
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager := &gke.GkeManagerMock{}

			var gkeNodeGroups []gke.NodeGroup
			for _, config := range tc.configs {
				gkeNodeGroups = append(gkeNodeGroups, newTestMig(gkeManager, config, false, tc.getTargetSizeErr))
			}

			capacity, err := getGkeNodeGroupsCapacity(gkeNodeGroups)

			if (err != nil) != tc.wantErr {
				t.Errorf("getGkeNodeGroupsCapacity() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			assert.Equal(t, tc.wantCapacity, capacity)
		})
	}
}
