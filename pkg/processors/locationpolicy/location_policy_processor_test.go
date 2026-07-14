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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	ca_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
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

	balancerCallOk := balancerCall{
		response: balancerResponse,
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
			mockProcessor.On("BalanceScaleUpBetweenGroups", mock.Anything, nodeGroups, tc.newNodes).Return(defaultResponse, nil).Once()

			balancers := make(map[gke.LocationPolicyEnum]Balancer)
			for policy, balancer := range tc.balancers {
				call, found := tc.balancerCalls[policy]
				if found {
					balancer.On("Balance", gkeNodeGroups, tc.newNodes).Return(call.response, call.err).Once()
				}
				balancers[policy] = balancer
			}

			processor := NewProcessor(mockProcessor, provider, balancers, experiments.NewMockManager(tc.enabledExperimentFlags...), false, nil, nil)
			scaleUpInfo, err := processor.BalanceScaleUpBetweenGroups(&context.AutoscalingContext{ProvisioningRequestScaleUpMode: tc.provisioningRequestScaleUpMode}, nodeGroups, tc.newNodes)

			for _, balancer := range tc.balancers {
				balancer.AssertExpectations(t)
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
func (m *nodeGroupSetProcessorMock) FindSimilarNodeGroups(_ *context.AutoscalingContext, _ cloudprovider.NodeGroup, _ map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, ca_errors.AutoscalerError) {
	return nil, nil
}

// BalanceScaleUpBetweenGroups is a mocked method of the NodeGroupSetProcessor interface.
func (m *nodeGroupSetProcessorMock) BalanceScaleUpBetweenGroups(context *context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, ca_errors.AutoscalerError) {
	args := m.Called(context, groups, newNodes)
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
