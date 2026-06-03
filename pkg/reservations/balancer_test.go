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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestBalanceScaleUpBetweenGroups(t *testing.T) {
	mig1 := newTestNodeGroup("machine-type-2", "zone-a", 5, 10)
	mig2 := newTestNodeGroup("machine-type-2", "zone-b", 3, 10)
	mig3 := newTestNodeGroup("machine-type-2", "zone-c", 6, 10)
	testCases := []struct {
		name               string
		groups             []cloudprovider.NodeGroup
		upcomingNodeGroups []cloudprovider.NodeGroup
		reservations       []*gce_api.Reservation
		newNodes           int
		wantScaleUpInfo    []nodegroupset.ScaleUpInfo
		wantReservations   []*gce_api.Reservation
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
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation("machine-type-2", "zone-a", 18, 20),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 7, 10),
			},
			wantReservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation("machine-type-2", "zone-a", 20, 20),
			},
		},
		{
			name:     "many groups, some reservations",
			groups:   []cloudprovider.NodeGroup{mig1, mig2, mig3},
			newNodes: 10,
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 10, 20, "machine-type-2", "zone-a"),
				BuildMultipleMachineReservationWithId(2, 0, 10, "machine-type-2", "zone-b"),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 9, 10),
				newTestScaleUpInfo(mig2, 3, 9, 10),
			},
			wantReservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 14, 20, "machine-type-2", "zone-a"),
				BuildMultipleMachineReservationWithId(2, 6, 10, "machine-type-2", "zone-b"),
			},
		},
		{
			name:   "resources for upcoming node groups are ignored when matching reservations",
			groups: []cloudprovider.NodeGroup{mig1},
			upcomingNodeGroups: []cloudprovider.NodeGroup{
				newUpcomingTestNodeGroup("", "machine-type-2", "zone-a", 10, 6),
			},
			newNodes: 10,
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 10, 20, "machine-type-2", "zone-a"),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{
				newTestScaleUpInfo(mig1, 5, 9, 10),
			},
			wantReservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 14, 20, "machine-type-2", "zone-a"),
			},
		},
		{
			name:   "resources for upcoming node groups are fully consumed by matching reservations",
			groups: []cloudprovider.NodeGroup{mig1},
			upcomingNodeGroups: []cloudprovider.NodeGroup{
				newUpcomingTestNodeGroup("", "machine-type-2", "zone-a", 10, 10),
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 10, 20, "machine-type-2", "zone-a"),
			},
			wantScaleUpInfo: []nodegroupset.ScaleUpInfo{},
			wantReservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 10, 20, "machine-type-2", "zone-a"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"zone-a, zone-b"}, nil })
			puller := gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
			puller.SetReservations(tc.reservations)

			provider := &gke.GkeCloudProviderMock{}
			provider.On("NodeGroups").Return(append(tc.groups, tc.upcomingNodeGroups...)).Once()

			processor := NewReservationBalancingProcessor(&testBalancingProcessor{}, puller, localssdsize.NewSimpleLocalSSDProvider(), provider)

			ctx := &autoscaling_context.AutoscalingContext{
				CloudProvider: provider,
			}
			scaleUpInfos, err := processor.BalanceScaleUpBetweenGroups(ctx, tc.groups, tc.newNodes)
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.wantScaleUpInfo, scaleUpInfos)
			assert.ElementsMatch(t, tc.wantReservations, puller.GetReservations())
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

type testBalancingProcessor struct {
}

func (p *testBalancingProcessor) FindSimilarNodeGroups(*autoscaling_context.AutoscalingContext, cloudprovider.NodeGroup, map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return nil, nil
}

func (p *testBalancingProcessor) BalanceScaleUpBetweenGroups(_ *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, _ int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	var scaleUpInfos []nodegroupset.ScaleUpInfo
	for _, group := range groups {
		currentSize, err := group.TargetSize()
		if err != nil {
			return scaleUpInfos, auto_errors.NewAutoscalerErrorf(auto_errors.CloudProviderError, "failed to get node group size: %v", err)
		}

		scaleUpInfos = append(scaleUpInfos, nodegroupset.ScaleUpInfo{
			Group:       group,
			CurrentSize: currentSize,
			NewSize:     currentSize,
			MaxSize:     group.MaxSize(),
		})
	}
	return scaleUpInfos, nil
}
func (p *testBalancingProcessor) CleanUp() {}

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
		reservations               []*gce_api.Reservation
		wantReservationsPostUpdate []*gce_api.Reservation
	}{
		{
			name:         "No scale up info",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation(matching, "zone-A", 0, 5),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservation(matching, "zone-A", 0, 5),
			},
		},
		{
			name: "Single zone scale-up info, single reservation is updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 0, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, matching, "zone-A"),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 3, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, matching, "zone-A"),
			},
		},
		{
			name: "Single zone scale-up info, multiple reservations update",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 8, MaxSize: 10},
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 0, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, matching, "zone-A"),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 3, 5, matching, "zone-A"),
			},
		},
		{
			name: "Single zone scale-up info, non-matching reservations are not updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservation(different, "zone-A", 0, 5),
				BuildMultipleMachineReservation(matching, "zone-B", 0, 5),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservation(different, "zone-A", 0, 5),
				BuildMultipleMachineReservation(matching, "zone-B", 0, 5),
			},
		},
		{
			name: "Multiple zone scale-up info, multiple reservations are updated",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 3, MaxSize: 10},
				{Group: migB, CurrentSize: 0, NewSize: 4, MaxSize: 10},
				{Group: migC, CurrentSize: 0, NewSize: 5, MaxSize: 10},
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 0, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, matching, "zone-B"),
				BuildMultipleMachineReservationWithId(3, 0, 5, matching, "zone-C"),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 3, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 4, 5, matching, "zone-B"),
				BuildMultipleMachineReservationWithId(3, 5, 5, matching, "zone-C"),
			},
		},
		{
			name: "Everything together",
			scaleUpInfos: []nodegroupset.ScaleUpInfo{
				{Group: migA, CurrentSize: 0, NewSize: 7, MaxSize: 10},
				{Group: migB, CurrentSize: 0, NewSize: 8, MaxSize: 10},
				{Group: migC, CurrentSize: 0, NewSize: 9, MaxSize: 10},
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 0, 5, different, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, different, "zone-B"),
				BuildMultipleMachineReservationWithId(3, 0, 5, different, "zone-C"),
				BuildMultipleMachineReservationWithId(4, 0, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(5, 0, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(6, 0, 5, matching, "zone-B"),
				BuildMultipleMachineReservationWithId(7, 0, 10, matching, "zone-C"),
			},
			wantReservationsPostUpdate: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 0, 5, different, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, different, "zone-B"),
				BuildMultipleMachineReservationWithId(3, 0, 5, different, "zone-C"),
				BuildMultipleMachineReservationWithId(4, 5, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(5, 2, 5, matching, "zone-A"),
				BuildMultipleMachineReservationWithId(6, 5, 5, matching, "zone-B"),
				BuildMultipleMachineReservationWithId(7, 9, 10, matching, "zone-C"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			processor := NewReservationBalancingProcessor(nil, nil, localssdsize.NewSimpleLocalSSDProvider(), provider)
			reservations := processor.updateReservations(tc.scaleUpInfos, tc.reservations, tc.reservations)

			if diff := cmp.Diff(reservations, tc.wantReservationsPostUpdate); diff != "" {
				t.Errorf("updateReservationsCache(nodegroup, usedReservations) diff (-want +got): %v %v %v", diff, reservations, tc.wantReservationsPostUpdate)
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
		reservations []*gce_api.Reservation
		want         []*gce_api.Reservation
	}{
		{
			name:       "no upcoming nodes - no adjustment to the reservations",
			nodeGroups: []cloudprovider.NodeGroup{newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
		},
		{
			name:       "adjust reservation only for upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 1, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 3, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
		},
		{
			name:       "multiple upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newUpcomingTestNodeGroup("np2", machineType1, "zone-A", 5, 1)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 1, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 4, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
		},
		{
			name:       "upcoming node without matching reservations does not adjust reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-C", 5, 2)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
		},
		{
			name:       "upcoming nodes with matching reservations and all matching reservations are already fully utilized",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
		},
		{
			name:       "upcoming nodes with matching reservations, but not enough reservations to accommodate all upcoming nodes",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 4, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
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
			if diff := cmp.Diff(reservations, tc.want); diff != "" {
				t.Errorf("consumeUpcomingScaleUps(context, reservations) diff (-want +got): %v %v %v", diff, reservations, tc.want)
			}
		})
	}
}

func TestReservationsForUpcomingNodes(t *testing.T) {
	machineType1 := "machine-type-1"
	testCases := []struct {
		name         string
		nodeGroups   []cloudprovider.NodeGroup
		reservations []*gce_api.Reservation
		want         map[uint64]int
	}{
		{
			name:       "no upcoming nodes",
			nodeGroups: []cloudprovider.NodeGroup{newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: map[uint64]int{},
		},
		{
			name:       "upcoming nodes with matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2), newTestNodeGroup(machineType1, "zone-A", 0, 10)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 1, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: map[uint64]int{
				1: 2,
			},
		},
		{
			name:       "upcoming nodes no matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-C", 5, 2)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-B"),
			},
			want: map[uint64]int{},
		},
		{
			name:       "upcoming nodes with multiple matching reservations",
			nodeGroups: []cloudprovider.NodeGroup{newUpcomingTestNodeGroup("np1", machineType1, "zone-A", 5, 2)},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 1, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-A"),
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
		reservations         []*gce_api.Reservation
		want                 []*gce_api.Reservation
	}{
		{
			name:                 "no consumed reservations",
			consumedReservations: map[uint64]int{},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-A"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 5, 1, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-A"),
			},
		},
		{
			name: "consumed reservations",
			consumedReservations: map[uint64]int{
				1: 2,
			},
			reservations: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 1, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-A"),
			},
			want: []*gce_api.Reservation{
				BuildMultipleMachineReservationWithId(1, 3, 5, machineType1, "zone-A"),
				BuildMultipleMachineReservationWithId(2, 0, 5, machineType1, "zone-A"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subtractedReservation := subtractReservations(tc.reservations, tc.consumedReservations)
			if diff := cmp.Diff(subtractedReservation, tc.want); diff != "" {
				t.Errorf("subtractReservations(consumedReservations, reservations) diff (-want +got): %v %v %v", diff, subtractedReservation, tc.want)
			}
		})
	}
}
