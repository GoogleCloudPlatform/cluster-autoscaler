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

package reservations

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/reservation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/testutil"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	autoscaling_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	auto_errors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

func TestBalanceScaleUpBetweenGroups(t *testing.T) {
	mig1 := newTestNodeGroup("machine-type-2", "zone-a", 5, 10)
	mig2 := newTestNodeGroup("machine-type-2", "zone-b", 3, 10)
	mig3 := newTestNodeGroup("machine-type-2", "zone-c", 6, 10)
	mig1000_1 := newTestNodeGroup("machine-type-2", "zone-a", 0, 1000)
	mig1000_2 := newTestNodeGroup("machine-type-2", "zone-b", 0, 1000)
	mig1000_3 := newTestNodeGroup("machine-type-2", "zone-c", 0, 1000)

	testCases := []struct {
		name                  string
		groups                []cloudprovider.NodeGroup
		withFallbackBalancers func(registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor
		upcomingNodeGroups    []cloudprovider.NodeGroup
		reservations          []*reservation.Reservation
		newNodes              int
		wantScaleUpInfo       []nodegroupset.ScaleUpInfo
		wantReservations      []*reservation.Reservation
	}{
		{
			name: "single group, no reservations",
			groups: []cloudprovider.NodeGroup{
				mig1,
			},
			newNodes:        10,
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{},
		},
		{
			name: "single group, some reservations",
			groups: []cloudprovider.NodeGroup{
				mig1,
			},
			newNodes: 10,
			reservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 2),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 7, 10),
			},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 0),
			},
		},
		{
			name:     "many groups, some reservations",
			groups:   []cloudprovider.NodeGroup{mig1, mig2, mig3},
			newNodes: 10,
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 10),
				BuildAvailableReservationWithID(2, "machine-type-2", "zone-b", 10),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 9, 10),
				newTestScaleUpInfo(mig2, 3, 9, 10),
			},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 6),
				BuildAvailableReservationWithID(2, "machine-type-2", "zone-b", 4),
			},
		},
		{
			name:   "resources for upcoming node groups are ignored when matching reservations",
			groups: []cloudprovider.NodeGroup{mig1},
			upcomingNodeGroups: []cloudprovider.NodeGroup{
				newUpcomingTestNodeGroup("", "machine-type-2", "zone-a", 10, 6),
			},
			newNodes: 10,
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 10),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 9, 10),
			},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 6),
			},
		},
		{
			name:   "resources for upcoming node groups are fully consumed by matching reservations",
			groups: []cloudprovider.NodeGroup{mig1},
			upcomingNodeGroups: []cloudprovider.NodeGroup{
				newUpcomingTestNodeGroup("", "machine-type-2", "zone-a", 10, 10),
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 10),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, "machine-type-2", "zone-a", 10),
			},
		},
		{
			name:     "newNodes are completely handled by available reservations - doesn't call fallback balancers",
			groups:   []cloudprovider.NodeGroup{mig1000_1, mig1000_2, mig1000_3},
			newNodes: 100,
			withFallbackBalancers: func(registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Panic("fallbackBalancer: should not be called")
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 100),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1000_1, 0, 100, 1000),
			},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 0),
			},
		},
		{
			name:     "newNodes are not completely handled by available reservations - calls fallback balancers",
			groups:   []cloudprovider.NodeGroup{mig1000_1, mig1000_2, mig1000_3},
			newNodes: 1000,
			withFallbackBalancers: func(registerMock func(m *mock.Mock)) nodegroupset.NodeGroupSetProcessor {
				mockBalancer := new(testutil.MockBalancer)
				mockBalancer.On("BalanceScaleUpBetweenGroups", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().Return(
					func(autoscalingCtx *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
						// use OSS balancer which should distribute remaining nodes
						ossBalancer := &nodegroupset.BalancingNodeGroupSetProcessor{}
						return ossBalancer.BalanceScaleUpBetweenGroups(context.TODO(), autoscalingCtx, groups, newNodes)
					},
				)
				registerMock(&mockBalancer.Mock)
				return mockBalancer
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 500),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1000_1, 0, 500, 1000),
				newTestScaleUpInfo(mig1000_2, 0, 250, 1000),
				newTestScaleUpInfo(mig1000_3, 0, 250, 1000),
			},
			wantReservations: []*reservation.Reservation{
				BuildAvailableReservation("machine-type-2", "zone-a", 0),
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

			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"zone-a, zone-b"}, nil })
			puller, _ := gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
			puller.SetReservations(tc.reservations)

			provider := &gke.GkeCloudProviderMock{}
			provider.On("NodeGroups").Return(append(tc.groups, tc.upcomingNodeGroups...)).Once()
			registerMock(&provider.Mock)

			fallbackBalancers := (nodegroupset.NodeGroupSetProcessor)(nil)
			if tc.withFallbackBalancers != nil {
				fallbackBalancers = tc.withFallbackBalancers(registerMock)
			} else {
				fallbackBalancers = &noopTestBalancingProcessor{}
			}
			processor := NewReservationBalancingProcessor(fallbackBalancers, puller, localssdsize.NewSimpleLocalSSDProvider(), provider)

			autoscalingCtx := &autoscaling_context.AutoscalingContext{
				CloudProvider: provider,
			}
			scaleUpInfos, err := processor.BalanceScaleUpBetweenGroups(context.TODO(), autoscalingCtx, tc.groups, tc.newNodes)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.wantScaleUpInfo), len(scaleUpInfos))
			assert.ElementsMatch(t, tc.wantScaleUpInfo, scaleUpInfos)
			assert.ElementsMatch(t, tc.wantReservations, puller.GetReservations())

			for _, m := range mocks {
				m.AssertExpectations(t)
			}
		})
	}
}

func TestDistributeNewNodes(t *testing.T) {
	mig1 := newTestNodeGroup("machine-type-2", "zone-a", 5, 10)
	mig2 := newTestNodeGroup("machine-type-2", "zone-b", 3, 10)
	mig3 := newTestNodeGroup("machine-type-2", "zone-c", 6, 10)
	mig4 := newTestNodeGroup("machine-type-2", "zone-d", 3, 5)
	testCases := []struct {
		name           string
		balancingInfos []*balancingInfo
		newNodes       int
		want           []*balancingInfo
		wantAddedNodes int
	}{
		{
			name: "no reservations",
			balancingInfos: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 5, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 3, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 0},
			},
			newNodes: 10,
			want: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 5, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 3, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 0},
			},
			wantAddedNodes: 0,
		},
		{
			name: "limited reservations in some zones not sufficient for the total scale up",
			balancingInfos: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 5, MaxSize: 10}, unUsedReservationCount: 3},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 3, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 4},
			},
			newNodes: 10,
			want: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 8, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 3, MaxSize: 10}, unUsedReservationCount: 0},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 10, MaxSize: 10}, unUsedReservationCount: 0},
			},
			wantAddedNodes: 7,
		},
		{
			name: "enough reservations in all zones, sufficient for the total scale up",
			balancingInfos: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 5, MaxSize: 10}, unUsedReservationCount: 10},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 3, MaxSize: 10}, unUsedReservationCount: 10},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 10},
			},
			newNodes: 4,
			want: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 9},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig2, CurrentSize: 3, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 7},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 10},
			},
			wantAddedNodes: 4,
		},
		{
			name: "enough reservations in all zones, respect node group max size",
			balancingInfos: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 5, MaxSize: 10}, unUsedReservationCount: 10},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig4, CurrentSize: 3, NewSize: 3, MaxSize: 5}, unUsedReservationCount: 10},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 6, MaxSize: 10}, unUsedReservationCount: 10},
			},
			newNodes: 5,
			want: []*balancingInfo{
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig1, CurrentSize: 5, NewSize: 7, MaxSize: 10}, unUsedReservationCount: 8},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig4, CurrentSize: 3, NewSize: 5, MaxSize: 5}, unUsedReservationCount: 8},
				{scaleUpInfo: nodegroupset.ScaleUpInfo{Group: mig3, CurrentSize: 6, NewSize: 7, MaxSize: 10}, unUsedReservationCount: 9},
			},
			wantAddedNodes: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotAddedNodes := distributeNewNodes(tc.balancingInfos, tc.newNodes)
			assert.ElementsMatch(t, tc.want, got)
			assert.Equal(t, tc.wantAddedNodes, gotAddedNodes)
		})
	}
}

// noopTestBalancingProcessor just returns passesd nodeGroups without changing any sizes
type noopTestBalancingProcessor struct {
}

func (p *noopTestBalancingProcessor) FindSimilarNodeGroups(context.Context, *autoscaling_context.AutoscalingContext, cloudprovider.NodeGroup, map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return nil, nil
}

func (p *noopTestBalancingProcessor) BalanceScaleUpBetweenGroups(ctx context.Context, _ *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	var scaleUpInfos []nodegroupset.ScaleUpInfo
	for _, group := range groups {
		currentSize, err := group.TargetSize(context.TODO())
		if err != nil {
			return scaleUpInfos, auto_errors.NewAutoscalerErrorf(auto_errors.CloudProviderError, "failed to get node group size: %v", err)
		}

		scaleUpInfos = append(scaleUpInfos, nodegroupset.ScaleUpInfo{
			Group:       group,
			CurrentSize: currentSize,
			NewSize:     currentSize,
			MaxSize:     group.MaxSize(context.TODO()),
		})
	}
	return scaleUpInfos, nil
}

func (p *noopTestBalancingProcessor) CleanUp() {}

func newTestNodeGroup(machineType, zone string, currentSize, maxSize int) cloudprovider.NodeGroup {
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetMigSize", mock.AnythingOfType("*gke.GkeMig")).Return(int64(currentSize), nil)
	return gke.NewTestGkeMigBuilder().SetGkeManager(gkeManagerMock).SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).SetGceRefZone(zone).SetMaxSize(maxSize).SetExist(true).Build()
}

func newTestScaleUpInfo(group cloudprovider.NodeGroup, currentSize, newSize, maxSize int) nodegroupset.ScaleUpInfo {
	return nodegroupset.ScaleUpInfo{
		Group:       group,
		CurrentSize: currentSize,
		NewSize:     newSize,
		MaxSize:     maxSize,
	}
}

func TestUpdateReservations(t *testing.T) {
	matching := "matching-machine-type"
	different := "different-machine-type"

	spec := &gkeclient.NodePoolSpec{MachineType: matching}
	migA := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRefZone("zone-A").Build()
	migB := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRefZone("zone-B").Build()
	migC := gke.NewTestGkeMigBuilder().SetSpec(spec).SetGceRefZone("zone-C").Build()

	testCases := []struct {
		name                       string
		scaleUpInfos               []nodegroupset.ScaleUpInfo
		reservations               []*reservation.Reservation
		wantReservationsPostUpdate []*reservation.Reservation
	}{
		{
			name:         "No scale up info",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{},
			reservations: []*reservation.Reservation{
				BuildAvailableReservation(matching, "zone-A", 5),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservation(matching, "zone-A", 5),
			},
		},
		{
			name: "Single zone scale-up info, single reservation is updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 5),
				BuildAvailableReservationWithID(2, matching, "zone-A", 5),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 2),
				BuildAvailableReservationWithID(2, matching, "zone-A", 5),
			},
		},
		{
			name: "Single zone scale-up info, multiple reservations update",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 8, MaxSize: 10},
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 5),
				BuildAvailableReservationWithID(2, matching, "zone-A", 5),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 0),
				BuildAvailableReservationWithID(2, matching, "zone-A", 2),
			},
		},
		{
			name: "Single zone scale-up info, non-matching reservations are not updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservation(different, "zone-A", 5),
				BuildAvailableReservation(matching, "zone-B", 5),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservation(different, "zone-A", 5),
				BuildAvailableReservation(matching, "zone-B", 5),
			},
		},
		{
			name: "Multiple zone scale-up info, multiple reservations are updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
				{Group: migB, CurrentSize: 0, NewSize: 4, MaxSize: 10},
				{Group: migC, CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 5),
				BuildAvailableReservationWithID(2, matching, "zone-B", 5),
				BuildAvailableReservationWithID(3, matching, "zone-C", 5),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, matching, "zone-A", 2),
				BuildAvailableReservationWithID(2, matching, "zone-B", 1),
				BuildAvailableReservationWithID(3, matching, "zone-C", 0),
			},
		},
		{
			name: "Everything together",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 7, MaxSize: 10},
				{Group: migB, CurrentSize: 0, NewSize: 8, MaxSize: 10},
				{Group: migC, CurrentSize: 0, NewSize: 9, MaxSize: 10},
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, different, "zone-A", 5),
				BuildAvailableReservationWithID(2, different, "zone-B", 5),
				BuildAvailableReservationWithID(3, different, "zone-C", 5),
				BuildAvailableReservationWithID(4, matching, "zone-A", 5),
				BuildAvailableReservationWithID(5, matching, "zone-A", 5),
				BuildAvailableReservationWithID(6, matching, "zone-B", 5),
				BuildAvailableReservationWithID(7, matching, "zone-C", 10),
			},
			wantReservationsPostUpdate: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, different, "zone-A", 5),
				BuildAvailableReservationWithID(2, different, "zone-B", 5),
				BuildAvailableReservationWithID(3, different, "zone-C", 5),
				BuildAvailableReservationWithID(4, matching, "zone-A", 0),
				BuildAvailableReservationWithID(5, matching, "zone-A", 3),
				BuildAvailableReservationWithID(6, matching, "zone-B", 0),
				BuildAvailableReservationWithID(7, matching, "zone-C", 1),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			processor := NewReservationBalancingProcessor(nil, nil, localssdsize.NewSimpleLocalSSDProvider(), provider)
			rsvs := tc.reservations
			reservations := processor.updateReservations(tc.scaleUpInfos, rsvs, rsvs)

			if diff := cmp.Diff(tc.wantReservationsPostUpdate, reservations); diff != "" {
				t.Errorf("updateReservations() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func newUpcomingTestNodeGroup(nodePoolName, machineType, zone string, maxSize, targetSize int) cloudprovider.NodeGroup {
	gkeManager := gke.GkeManagerMock{MockIsUpcoming: true}
	mig := gke.NewTestGkeMigBuilder().SetGkeManager(&gkeManager).SetNodePoolName(nodePoolName).SetSpec(&gkeclient.NodePoolSpec{MachineType: machineType}).SetGceRefZone(zone).SetGceRefName(nodePoolName).SetMaxSize(maxSize).Build()
	gkeManager.On("IsUpcoming", mig).Return(true)
	gkeManager.On("GetMigSize", mig).Return(int64(targetSize), nil)
	return mig
}

func TestConsumeUpcomingScaleUps(t *testing.T) {
	machineType1 := "machine-type-1"
	testCases := []struct {
		name         string
		nodeGroups   []cloudprovider.NodeGroup
		reservations []*reservation.Reservation
		want         []*reservation.Reservation
	}{
		{
			name:       "no upcoming nodes - no adjustment to the reservations",
			nodeGroups: []cloudprovider.NodeGroup{newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
		{
			name:       "adjust reservation only for upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 4),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 2),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
		{
			name:       "multiple upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newUpcomingTestNodeGroup("np2", machineType1, "zone-A", 5, 1)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 4),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 1),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
		{
			name:       "upcoming node without matching reservations does not adjust reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-C", 5, 2)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
		{
			name:       "upcoming nodes with matching reservations and all matching reservations are already fully utilized",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
		{
			name:       "upcoming nodes with matching reservations, but not enough reservations to accommodate all upcoming nodes",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 1),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &gke.GkeCloudProviderMock{}
			provider.On("NodeGroups").Return(tc.nodeGroups).Once()
			processor := NewReservationBalancingProcessor(nil, nil, localssdsize.NewSimpleLocalSSDProvider(), provider)

			ctx := &autoscaling_context.AutoscalingContext{
				CloudProvider: provider,
			}
			reservations := processor.consumeUpcomingScaleUps(ctx, tc.reservations)
			if diff := cmp.Diff(tc.want, reservations); diff != "" {
				t.Errorf("consumeUpcomingScaleUps() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReservationsForUpcomingNodes(t *testing.T) {
	machineType1 := "machine-type-1"
	testCases := []struct {
		name         string
		nodeGroups   []cloudprovider.NodeGroup
		reservations []*reservation.Reservation
		want         map[uint64]int
	}{
		{
			name:       "no upcoming nodes",
			nodeGroups: []cloudprovider.NodeGroup{newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: map[uint64]int{},
		},
		{
			name:       "upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 4),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: map[uint64]int{
				1: 2,
			},
		},
		{
			name:       "upcoming nodes no matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-C", 5, 2)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-B", 5),
			},
			want: map[uint64]int{},
		},
		{
			name:       "upcoming nodes with multiple matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 4),
				BuildAvailableReservationWithID(2, machineType1, "zone-A", 5),
			},
			want: map[uint64]int{
				1: 2,
				2: 0,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &gke.GkeCloudProviderMock{}
			provider.On("NodeGroups").Return(tc.nodeGroups).Once()
			processor := NewReservationBalancingProcessor(nil, nil, localssdsize.NewSimpleLocalSSDProvider(), provider)

			ctx := &autoscaling_context.AutoscalingContext{
				CloudProvider: provider,
			}
			consumedReservations := processor.reservationsForUpcomingNodes(ctx, tc.reservations)
			if diff := cmp.Diff(consumedReservations, tc.want); diff != "" {
				t.Errorf("reservationsForUpcomingNodes(context, reservations) diff (-want +got): %v %v %v", diff, consumedReservations, tc.want)
			}
		})
	}
}

func TestAdjustReservations(t *testing.T) {
	machineType1 := "machine-type-1"
	testCases := []struct {
		name                 string
		consumedReservations map[uint64]int
		reservations         []*reservation.Reservation
		want                 []*reservation.Reservation
	}{
		{
			name:                 "no consumed reservations",
			consumedReservations: map[uint64]int{},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-A", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 0),
				BuildAvailableReservationWithID(2, machineType1, "zone-A", 5),
			},
		},
		{
			name: "consumed reservations",
			consumedReservations: map[uint64]int{
				1: 2,
			},
			reservations: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 4),
				BuildAvailableReservationWithID(2, machineType1, "zone-A", 5),
			},
			want: []*reservation.Reservation{
				BuildAvailableReservationWithID(1, machineType1, "zone-A", 2),
				BuildAvailableReservationWithID(2, machineType1, "zone-A", 5),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rsvs := tc.reservations
			wantInput := make([]*reservation.Reservation, len(rsvs))
			for i, r := range rsvs {
				c, specific := *r, *r.SpecificReservation
				c.SpecificReservation = &specific
				wantInput[i] = &c
			}

			subtractedReservation := subtractReservations(rsvs, tc.consumedReservations)

			if diff := cmp.Diff(tc.want, subtractedReservation); diff != "" {
				t.Errorf("subtractReservations() diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(wantInput, rsvs); diff != "" {
				t.Errorf("subtractReservations() modified input reservations, diff (-want +got):\n%s", diff)
			}
		})
	}
}
