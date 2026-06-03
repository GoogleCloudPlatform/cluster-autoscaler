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

package balancer_chain

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/testutil"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/locationpolicy"
	"k8s.io/utils/ptr"
)

type rlaBalancerMock struct {
	mock.Mock
}

// Balance is a mocked method of the Balancer interface.
func (m *rlaBalancerMock) Balance(groups []gke.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, error) {
	args := m.Called(groups, newNodes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]nodegroupset.ScaleUpInfo), args.Error(1)
}

type balancersChain struct {
	fa1Balancer      nodegroupset.NodeGroupSetProcessor
	locationBalancer nodegroupset.NodeGroupSetProcessor
	fa2Balancer      nodegroupset.NodeGroupSetProcessor
	ossBalancer      *testutil.MockBalancer

	rootBalancer nodegroupset.NodeGroupSetProcessor

	// it's not the same type of balancers as all the other.. naming is not consistent
	rlaService *rlaBalancerMock
}

func TestBalancerChainScaleUpAndNotifying(t *testing.T) {
	locationBalancedMig1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMig2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMig3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyBalanced, 100, 1000, nil)

	locationAnyMig1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMig2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMig3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyAny, 100, 1000, nil)

	locationBalancedMigNoCCC1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMigNoCCC2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMigNoCCC3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "", gke.LocationPolicyBalanced, 100, 1000, nil)

	locationBalancedMigInvalidCCC1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-doesnt-exist", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMigInvalidCCC2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-doesnt-exist", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMigInvalidCCC3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-doesnt-exist", gke.LocationPolicyBalanced, 100, 1000, nil)

	locationAnyMigNoCCC1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMigNoCCC2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMigNoCCC3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "", gke.LocationPolicyAny, 100, 1000, nil)

	locationAnyMigInvalidCCC1 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-a", "ccc-doesnt-exist", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMigInvalidCCC2 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-b", "ccc-doesnt-exist", gke.LocationPolicyAny, 0, 1000, nil)
	locationAnyMigInvalidCCC3 := createMockMigs(&gke.GkeManagerMock{}, "e2-standard-4", "us-central1-c", "ccc-doesnt-exist", gke.LocationPolicyAny, 100, 1000, nil)

	getSnapshotForProvider := func(iaProvider *instanceavailability.MockProvider) *instanceavailability.Snapshot {
		return instanceavailability.NewSnapshot(iaProvider, "ccc-1", "", "guidance-1", map[string]int{"us-central1-a": 100, "us-central1-b": 100, "us-central1-c": 0}, map[string]float64{"us-central1-a": 1.0, "us-central1-b": 0.8, "us-central1-c": 0.8})
	}
	getScaleUpInfos := func(mig1, mig2, mig3 *gke.GkeMig) []nodegroupset.ScaleUpInfo {
		return []nodegroupset.ScaleUpInfo{
			{
				Group:       mig1,
				CurrentSize: 0,
				NewSize:     91,
				MaxSize:     1000,
			},
			{
				Group:       mig2,
				CurrentSize: 0,
				NewSize:     91,
				MaxSize:     1000,
			},
		}
	}

	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ccc-1",
		},
	}, "", false, nil, nil)

	http403Err := &googleapi.Error{
		Code:    403,
		Message: "no capacity",
	}
	rlaNonHttp403Err := fmt.Errorf("RLA unavailable")

	testCases := []struct {
		name                 string
		withMigs             func() []cloudprovider.NodeGroup
		newNodes             int
		initialSetup         func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider)
		isFlexAdvisorEnabled bool
		wantScaleUpInfos     []nodegroupset.ScaleUpInfo
		wantMarkUsedCalls    int
		wantErr              string
	}{
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA (http 403) -> FA2; notifies GCE once",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, http403Err).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Once().Return(getSnapshotForProvider(iaProvider), nil)
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(nil)
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA (non-http 403 error) -> FA2; notifies GCE once",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, rlaNonHttp403Err).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Once().Return(getSnapshotForProvider(iaProvider), nil)
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(nil)
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
		// TODO: test for when locationBalancer fails (instead of RLA like rest of tests) - doesnt notify GCE
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA; notifies GCE once",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", "ccc-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Once().Return(getSnapshotForProvider(iaProvider), nil)
				iaProvider.On("MarkUsed", "ccc-1", "", "guidance-1", mock.Anything, map[string]int{"us-central1-a": 91, "us-central1-b": 91}).Once().Return(nil)
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA; mig without CCC, doesnt notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA; non-existent CCC, doesnt notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMigInvalidCCC1, locationAnyMigInvalidCCC2, locationAnyMigInvalidCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMigInvalidCCC1, locationAnyMigInvalidCCC2, locationAnyMigInvalidCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("value not found")).Once()
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMigInvalidCCC1, locationAnyMigInvalidCCC2, locationAnyMigInvalidCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA (http 403) -> FA2 -> OSS; mig without CCC, doesnt notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, http403Err).Once()
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA (non-http 403 error) -> FA2 -> OSS; mig without CCC, doesn't notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, rlaNonHttp403Err).Once()
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMigNoCCC1, locationAnyMigNoCCC2, locationAnyMigNoCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==ANY; FA1 -> RLA (non-http 403 error) -> FA2; notifies GCE once",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, rlaNonHttp403Err).Once()
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("OSS balancer: should not be called")
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Return(getSnapshotForProvider(iaProvider), nil).Once()
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(nil)
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==BALANCED; FA1 -> FA2 -> OSS; mig without CCC, doesnt notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationBalancedMigNoCCC1, locationBalancedMigNoCCC2, locationBalancedMigNoCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Maybe().Panic("should not be called")
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationBalancedMigNoCCC1, locationBalancedMigNoCCC2, locationBalancedMigNoCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationBalancedMigNoCCC1, locationBalancedMigNoCCC2, locationBalancedMigNoCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==BALANCED; FA1 -> FA2 -> OSS; non-existent CCC, doesnt notify GCE",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationBalancedMigInvalidCCC1, locationBalancedMigInvalidCCC2, locationBalancedMigInvalidCCC3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Maybe().Panic("should not be called")
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationBalancedMigInvalidCCC1, locationBalancedMigInvalidCCC2, locationBalancedMigInvalidCCC3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("value not found")).Twice()
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationBalancedMigInvalidCCC1, locationBalancedMigInvalidCCC2, locationBalancedMigInvalidCCC3),
		},
		{
			name:                 "FlexAdvisorEnabled=true, locationPolicy==BALANCED; FA1 (doesnt fallback); CCC found, notifies GCE once",
			isFlexAdvisorEnabled: true,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationBalancedMig1, locationBalancedMig2, locationBalancedMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Maybe().Panic("should not be called")
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("OSS balancer: should not be called")
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything).Once().Return(getSnapshotForProvider(iaProvider), nil)
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(nil)
			},
			wantScaleUpInfos: getScaleUpInfos(locationBalancedMig1, locationBalancedMig2, locationBalancedMig3),
		},
		{
			name:                 "FlexAdvisorEnabled=false, locationPolicy==ANY; RLA (doesnt fallback); doesnt notify GCE",
			isFlexAdvisorEnabled: false,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3), nil).Once()
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("OSS balancer: should not be called")
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
		{
			name:                 "FlexAdvisorEnabled=false, locationPolicy==ANY; RLA (http 403) -> OSS; doesnt notify GCE",
			isFlexAdvisorEnabled: false,
			newNodes:             182,
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationAnyMig1, locationAnyMig2, locationAnyMig3}
			},
			initialSetup: func(balancersChain *balancersChain, iaProvider *instanceavailability.MockProvider) {
				balancersChain.rlaService.On("Balance", mock.Anything, mock.Anything).Return(nil, http403Err).Once()
				balancersChain.ossBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3), nil).Once()
				iaProvider.On("AwaitInstanceAvailability", mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("AwaitInstanceAvailability: should not be called")
				iaProvider.On("MarkUsed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("MarkUsed: should not be called")
			},
			wantScaleUpInfos: getScaleUpInfos(locationAnyMig1, locationAnyMig2, locationAnyMig3),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gkeManager := &gke.GkeManagerMock{}

			gkeProvider, _ := gke.BuildGkeCloudProvider(gkeManager, nil, nil, true, "us-test1", nil, false, false, nil, "", nil, nil, nil, 1000)

			iaProvider := new(instanceavailability.MockProvider)
			iaProvider.On("IncrementFlexAdvisorCacheQueryCount", mock.Anything, mock.Anything, mock.Anything).Maybe().Return()
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1}, labels.ComputeClassLabel)

			balancersChain := balancersChain{}
			if tc.isFlexAdvisorEnabled {
				rlaService := new(rlaBalancerMock)
				balancersChain.ossBalancer = new(testutil.MockBalancer)
				balancersChain.fa2Balancer = flexadvisor.NewScaleUpBalancer(balancersChain.ossBalancer, iaProvider, mockLister, experiments.NewMockManager(), true)
				balancersChain.locationBalancer = locationpolicy.NewProcessor(balancersChain.fa2Balancer, gkeProvider, map[gke.LocationPolicyEnum]locationpolicy.Balancer{
					gke.LocationPolicyAny: rlaService,
				}, experiments.NewMockManager(), true, iaProvider, mockLister)
				balancersChain.fa1Balancer = flexadvisor.NewScaleUpBalancer(balancersChain.locationBalancer, iaProvider, mockLister, experiments.NewMockManager(), false)

				balancersChain.rootBalancer = balancersChain.fa1Balancer
				balancersChain.rlaService = rlaService
			} else {
				rlaService := new(rlaBalancerMock)
				balancersChain.ossBalancer = new(testutil.MockBalancer)
				balancersChain.locationBalancer = locationpolicy.NewProcessor(balancersChain.ossBalancer, gkeProvider, map[gke.LocationPolicyEnum]locationpolicy.Balancer{
					gke.LocationPolicyAny: rlaService,
				}, experiments.NewMockManager(), false, nil, nil)

				balancersChain.rootBalancer = balancersChain.locationBalancer
				balancersChain.rlaService = rlaService
			}

			tc.initialSetup(&balancersChain, iaProvider)
			got, gotErr := balancersChain.rootBalancer.BalanceScaleUpBetweenGroups(&ca_context.AutoscalingContext{
				ProvisioningRequestScaleUpMode: false,
			}, tc.withMigs(), tc.newNodes)

			balancersChain.rlaService.AssertExpectations(t)
			balancersChain.ossBalancer.AssertExpectations(t)
			iaProvider.AssertExpectations(t)

			assert.ElementsMatch(t, got, tc.wantScaleUpInfos)
			if tc.wantErr != "" {
				assert.NotNil(t, gotErr)
				assert.Contains(t, gotErr.Error(), tc.wantErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func buildNodeWithLabels(labels map[string]string) *apiv1.Node {
	node := apiv1.Node{}
	node.Labels = labels
	return &node
}

func createMockMigs(gkeMock *gke.GkeManagerMock, machineType, zone, cccName string, locationPolicy gke.LocationPolicyEnum, migSize, maxSize int, migSizeErr error) *gke.GkeMig {
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

	return mig
}

type mockAdviceProvider struct {
	mock.Mock
}

func (m *mockAdviceProvider) FetchCapacityGuidance(ctx context.Context, flexibilityScopeKey string, instanceConfigs map[string]*api.InstanceConfig) (availability map[string]*api.InstanceAvailability, err error) {
	args := m.Called(ctx, flexibilityScopeKey, instanceConfigs)
	if args.Get(0) != nil {
		availability = args.Get(0).(map[string]*api.InstanceAvailability)
	}
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return
}

func (m *mockAdviceProvider) SendCapacityDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) (err error) {
	args := m.Called(ctx, decision)
	if args.Get(0) != nil {
		err = args.Get(0).(error)
	}
	return
}

func TestBalancerChainScaleUpTimeoutFallback_GceClient(t *testing.T) {
	// --- 1. Arrange: GKE Mock Setup ---
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsResizeRequestErrorHandlingEnabled").Return(false)
	gkeManager.On("GetAutoprovisioningLocations").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}).Maybe()
	gkeManager.On("GetMachineType", mock.Anything, mock.Anything).Return(gce.MachineType{}, nil).Maybe()
	gkeManager.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2).Maybe()

	locationBalancedMig1 := createMockMigs(gkeManager, "e2-standard-4", "us-central1-a", "ccc-1", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMig2 := createMockMigs(gkeManager, "e2-standard-4", "us-central1-b", "ccc-1", gke.LocationPolicyBalanced, 0, 1000, nil)
	locationBalancedMig3 := createMockMigs(gkeManager, "e2-standard-4", "us-central1-c", "ccc-1", gke.LocationPolicyBalanced, 100, 1000, nil)

	getScaleUpInfos := func(mig1, mig2, mig3 *gke.GkeMig) []nodegroupset.ScaleUpInfo {
		return []nodegroupset.ScaleUpInfo{
			{
				Group:       mig1,
				CurrentSize: 0,
				NewSize:     91,
				MaxSize:     1000,
			},
			{
				Group:       mig2,
				CurrentSize: 0,
				NewSize:     91,
				MaxSize:     1000,
			},
		}
	}

	testCases := []struct {
		name             string
		withMigs         func() []cloudprovider.NodeGroup
		newNodes         int
		initialSetup     func(mockBalancer *testutil.MockBalancer, mockAdviceProvider *mockAdviceProvider, ctx context.Context)
		wantScaleUpInfos []nodegroupset.ScaleUpInfo
	}{
		{
			name: "FlexAdvisorEnabled=true, locationPolicy=BALANCED, AwaitInstanceAvailability times out, falls back to fallback balancer",
			withMigs: func() []cloudprovider.NodeGroup {
				return []cloudprovider.NodeGroup{locationBalancedMig1, locationBalancedMig2, locationBalancedMig3}
			},
			newNodes: 182,
			initialSetup: func(mockBalancer *testutil.MockBalancer, mockAdviceProvider *mockAdviceProvider, ctx context.Context) {
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything).Return(getScaleUpInfos(locationBalancedMig1, locationBalancedMig2, locationBalancedMig3), nil).Once()

				// Mock FetchCapacityGuidance to simulate high latency from GCE (exceeding the timeout threshold)
				mockAdviceProvider.On("FetchCapacityGuidance", mock.Anything, "ccc-1", mock.Anything).Run(func(args mock.Arguments) {
					select {
					// this blocks execution until end of test
					case <-ctx.Done():
					}
				}).Return(nil, fmt.Errorf("GCE timeout")).Once()
			},
			wantScaleUpInfos: getScaleUpInfos(locationBalancedMig1, locationBalancedMig2, locationBalancedMig3),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				gkeProvider, _ := gke.BuildGkeCloudProvider(gkeManager, nil, nil, true, "us-test1", nil, false, false, nil, "", nil, machinetypes.NewMachineConfigProvider(nil), nil, 1000)

				// --- 2. Arrange: ComputeClass CRD & Lister Setup ---
				mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{
					ccc.NewCccCrd(&v1.ComputeClass{
						ObjectMeta: metav1.ObjectMeta{
							Name: "ccc-1",
						},
						Spec: v1.ComputeClassSpec{
							Priorities: []v1.Priority{
								{
									MachineType: ptr.To("e2-standard-4"),
								},
							},
						},
					}, "", false, crd.TestDefaultDataProvider(), nil),
				}, labels.ComputeClassLabel)
				mockAdviceProvider := &mockAdviceProvider{}

				fa, err := flexadvisor.NewFlexAdvisor(ctx, mockAdviceProvider, mockLister, gkeProvider, optstracking.EmptyFakeOptionsTracker())
				assert.NoError(t, err)

				mockBalancer := new(testutil.MockBalancer)
				faBalancer := flexadvisor.NewScaleUpBalancer(mockBalancer, fa, mockLister, experiments.NewMockManager(), false)

				tc.initialSetup(mockBalancer, mockAdviceProvider, ctx)

				got, gotErr := faBalancer.BalanceScaleUpBetweenGroups(&ca_context.AutoscalingContext{
					ProvisioningRequestScaleUpMode: false,
				}, tc.withMigs(), tc.newNodes)

				assert.NoError(t, gotErr)
				assert.Equal(t, tc.wantScaleUpInfos, got)
				mockAdviceProvider.AssertExpectations(t)
				mockBalancer.AssertExpectations(t)
			})
		})
	}
}
