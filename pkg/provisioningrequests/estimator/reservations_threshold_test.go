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

package estimator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestReservationsThresholdNodeLimit(t1 *testing.T) {
	firstZone := "zone-A"
	defaultMachineType := "n1-standard-1"
	secondZone := "zone-B"
	thirdZone := "zone-C"

	type nodeGroupConfig struct {
		maxNodes   int
		nodesCount int
		zone       string
		affinity   string
	}

	type reservationsConfig struct {
		nodeGroupUsedCapacity  int
		nodeGroupTotalCapacity int
		zone                   string
	}

	// We assume that no 2 MIGs are in the same zone, i.e. no reservation matches 2 MIGs
	// Reservations and similar node groups have same machine type

	tests := []struct {
		name                    string
		reservationsConfig      []reservationsConfig
		thisNodeGroupConfig     nodeGroupConfig
		similarNodeGroupsConfig []nodeGroupConfig
		emptyContext            bool
		wantThreshold           int
		experimentEnabled       *bool
	}{
		{
			name:                "Returns 0 for no reservations",
			reservationsConfig:  []reservationsConfig{},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone},
			wantThreshold:       0,
		},
		{
			name: "Returns 0 for no capacity in the node group",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 1, nodeGroupTotalCapacity: 2, zone: firstZone}, // 1
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 0, nodesCount: 0, zone: firstZone},
			wantThreshold:       0,
		},
		{
			name: "Returns reservations for this node group having no context",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 1, nodeGroupTotalCapacity: 2, zone: firstZone}, // 1
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 3, zone: secondZone},
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 5, zone: thirdZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone},
			emptyContext:        true,
			wantThreshold:       1,
		},
		{
			name: "Threshold based on this node group reservations hits max node limit",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 1, nodeGroupTotalCapacity: 2, zone: firstZone}, // 1
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 3, zone: secondZone},
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 2, zone: thirdZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone},
			emptyContext:        true,
			wantThreshold:       1,
		},
		{
			name: "Computes threshold based on sng reservations",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 1, nodeGroupTotalCapacity: 2, zone: firstZone},  // 1
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 3, zone: secondZone}, // 3
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 5, zone: thirdZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 0, nodesCount: 0, zone: firstZone},
			similarNodeGroupsConfig: []nodeGroupConfig{
				{maxNodes: 10, nodesCount: 0, zone: firstZone},
				{maxNodes: 5, nodesCount: 0, zone: secondZone},
			},
			wantThreshold: 4,
		},
		{
			name: "Threshold based on sng reservations hits max node limit",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 10, zone: firstZone},  // 1
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 30, zone: secondZone}, // 3
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 5, zone: thirdZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 0, nodesCount: 0, zone: firstZone},
			similarNodeGroupsConfig: []nodeGroupConfig{
				{maxNodes: 2, nodesCount: 0, zone: firstZone},
				{maxNodes: 3, nodesCount: 0, zone: secondZone},
			},
			wantThreshold: 5,
		},
		{
			name: "Computes threshold based on combined reservations",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 10, nodeGroupTotalCapacity: 20, zone: firstZone},
				{nodeGroupUsedCapacity: 10, nodeGroupTotalCapacity: 50, zone: secondZone},
				{nodeGroupUsedCapacity: 10, nodeGroupTotalCapacity: 70, zone: thirdZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone},
			similarNodeGroupsConfig: []nodeGroupConfig{
				{maxNodes: 5, nodesCount: 0, zone: secondZone},
				{maxNodes: 100, nodesCount: 0, zone: thirdZone},
			},
			wantThreshold: 70,
		},
		{
			name:                "Returns_-1_for_no_reservations_with_any-then-fail_affinity",
			reservationsConfig:  []reservationsConfig{},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone, affinity: reservations.AnyThenFail},
			wantThreshold:       -1,
		},
		{
			name: "Returns_reservations_for_any-then-fail_affinity_when_available",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 3, zone: firstZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone, affinity: reservations.AnyThenFail},
			wantThreshold:       3,
		},
		{
			name: "Returns_0_for_none_affinity_even_when_reservations_are_available",
			reservationsConfig: []reservationsConfig{
				{nodeGroupUsedCapacity: 0, nodeGroupTotalCapacity: 3, zone: firstZone},
			},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone, affinity: reservations.NoneAffinity},
			wantThreshold:       0,
		},
		{
			name:                "Returns_-1_for_no_reservations_with_any-then-fail_affinity_experiment_enabled",
			reservationsConfig:  []reservationsConfig{},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone, affinity: reservations.AnyThenFail},
			wantThreshold:       -1,
			experimentEnabled:   func() *bool { b := true; return &b }(),
		},
		{
			name:                "Returns_0_for_no_reservations_with_any-then-fail_affinity_experiment_disabled",
			reservationsConfig:  []reservationsConfig{},
			thisNodeGroupConfig: nodeGroupConfig{maxNodes: 5, nodesCount: 0, zone: firstZone, affinity: reservations.AnyThenFail},
			wantThreshold:       0,
			experimentEnabled:   func() *bool { b := false; return &b }(),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{firstZone}, nil })
			gkeManagerMock := &gke.GkeManagerMock{}
			puller, err := gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, firstZone)
			assert.NoError(t1, err)

			res := make([]*gce_api.Reservation, 0)
			for i, resConfig := range tt.reservationsConfig {
				res = append(res, reservations.BuildMultipleMachineReservationWithId(i, resConfig.nodeGroupUsedCapacity, resConfig.nodeGroupTotalCapacity, defaultMachineType, resConfig.zone))
			}
			puller.SetReservations(res)

			similarNodeGroups := make([]cloudprovider.NodeGroup, 0)
			for _, ng := range tt.similarNodeGroupsConfig {
				mig := newTestGkeMig(gkeManagerMock, ng.zone, defaultMachineType, "DefaultNodePool", ng.maxNodes, ng.affinity)
				similarNodeGroups = append(similarNodeGroups, mig)
				gkeManagerMock.On("GetMigSize", mig).Return(int64(ng.nodesCount), nil)
			}
			var context estimator.EstimationContext
			if !tt.emptyContext {
				context = estimator.NewEstimationContext(0, similarNodeGroups, 0)
			}

			thisNodeGroup := newTestGkeMig(gkeManagerMock, tt.thisNodeGroupConfig.zone, defaultMachineType, "DefaultNodePool", tt.thisNodeGroupConfig.maxNodes, tt.thisNodeGroupConfig.affinity)
			gkeManagerMock.On("GetMigSize", thisNodeGroup).Return(int64(tt.thisNodeGroupConfig.nodesCount), nil)

			cloudProvider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			var optionsTracker *optstracking.OptionsTracker
			if tt.experimentEnabled != nil {
				manager := experiments.NewMockManager(experiments.AnyThenFailReservationAffinityThresholdEnabledFlag)
				if !*tt.experimentEnabled {
					manager.DisableAllExperiments()
				}
				optionsTracker = optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, manager)
			}

			t := &reservationsThreshold{
				reservationsPuller:       puller,
				localSSDDiskSizeProvider: localssdsize.NewSimpleLocalSSDProvider(),
				cloudProvider:            cloudProvider,
				optionsTracker:           optionsTracker,
			}

			if got := t.NodeLimit(thisNodeGroup, context); got.Limit != tt.wantThreshold {
				t1.Errorf("NodeLimit() = %v, want %v", got.Limit, tt.wantThreshold)
			}
		})
	}
}

func newTestGkeMig(gkeManager gke.GkeManager, zone string, machineType string, nodePoolName string, maxSize int, affinity string) *gke.GkeMig {
	if affinity == "" {
		affinity = reservations.AnyAffinity
	}
	gkeAffinity, ok := reservations.GkeAffinityFromSelectorValue(affinity)
	if !ok {
		gkeAffinity = gkeclient.ReservationAffinityAny
	}
	return gke.NewTestGkeMigBuilder().
		SetGkeManager(gkeManager).
		SetGceRef(gce.GceRef{
			Project: "project",
			Zone:    zone,
			Name:    nodePoolName,
		}).
		SetMaxSize(maxSize).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: machineType,
			Labels: map[string]string{
				gkelabels.ReservationAffinityLabel: affinity,
			},
			ReservationAffinity: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeAffinity,
			},
			// Required by gkeCloudProviderImpl
			LocalSSDConfig: &gkeclient.LocalSSDConfig{LocalSsdCount: 0},
		}).
		SetExist(true).
		SetAutoprovisioned(true).
		SetNodePoolName(nodePoolName).
		Build()
}
