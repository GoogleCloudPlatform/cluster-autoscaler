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

package nodeinfosprovider

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestUpdateCoreDistribution(t *testing.T) {
	allZones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}

	testCases := []struct {
		name                      string
		oldCoreDistribution       map[distributionConfig]distribution
		getTargetSizeErr          bool
		nodeInfoMissing           bool
		migConfigs                []testMigConfig
		wantUpdateMetricCalls     []updateMetricsCall
		wantUnregisterMetricCalls []unregisterMetricsCall
		wantCoreDistribution      map[distributionConfig]distribution
	}{
		{
			name: "no migs",
		},
		{
			name: "single mig",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 40},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
			},
		},
		{
			name: "many migs, identical",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 4},
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 68},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{68, 0, 0}),
			},
		},
		{
			name: "many migs, different zones",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
				{zone: "us-central1-b", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 4},
				{zone: "us-central1-c", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 40},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 16},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 12},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 16, 12}),
			},
		},
		{
			name: "many migs, different machine types",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
				{zone: "us-central1-a", machineType: "n1-standard-8", locationPolicy: gke.LocationPolicyBalanced, targetSize: 4},
				{zone: "us-central1-a", machineType: "n1-standard-16", locationPolicy: gke.LocationPolicyBalanced, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 40},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-8", "location_policy": "BALANCED"}, count: 32},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-8", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-8", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-16", "location_policy": "BALANCED"}, count: 48},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-16", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-16", "location_policy": "BALANCED"}},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}:  newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "BALANCED"}:  newTestDistribution(allZones, []int64{32, 0, 0}),
				{machineType: "n1-standard-16", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{48, 0, 0}),
			},
		},
		{
			name: "many migs, different location policies",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyAny, targetSize: 4},
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyUnspecified, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 40},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "ANY"}, count: 16},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}, count: 12},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}:                    newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-4", locationPolicy: "ANY"}:                         newTestDistribution(allZones, []int64{16, 0, 0}),
				{machineType: "n1-standard-4", locationPolicy: "LOCATION_POLICY_UNSPECIFIED"}: newTestDistribution(allZones, []int64{12, 0, 0}),
			},
		},
		{
			name: "many migs, different everything",
			migConfigs: []testMigConfig{
				{zone: "us-central1-a", machineType: "n1-standard-4", locationPolicy: gke.LocationPolicyBalanced, targetSize: 10},
				{zone: "us-central1-b", machineType: "n1-standard-8", locationPolicy: gke.LocationPolicyAny, targetSize: 4},
				{zone: "us-central1-c", machineType: "n1-standard-16", locationPolicy: gke.LocationPolicyUnspecified, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}, count: 40},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-8", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-8", "location_policy": "ANY"}, count: 32},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-8", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}, count: 48},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}:                     newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:                          newTestDistribution(allZones, []int64{0, 32, 0}),
				{machineType: "n1-standard-16", locationPolicy: "LOCATION_POLICY_UNSPECIFIED"}: newTestDistribution(allZones, []int64{0, 0, 48}),
			},
		},
		{
			name: "mig changes, node distribution updated",
			oldCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:      newTestDistribution(allZones, []int64{0, 0, 0}),
			},
			migConfigs: []testMigConfig{
				{zone: "us-central1-b", machineType: "n1-standard-8", locationPolicy: gke.LocationPolicyAny, targetSize: 4},
				{zone: "us-central1-c", machineType: "n1-standard-16", locationPolicy: gke.LocationPolicyUnspecified, targetSize: 3},
			},
			wantUpdateMetricCalls: []updateMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-8", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-8", "location_policy": "ANY"}, count: 32},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-8", "location_policy": "ANY"}},
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-16", "location_policy": "LOCATION_POLICY_UNSPECIFIED"}, count: 48},
			},
			wantUnregisterMetricCalls: []unregisterMetricsCall{
				{labels: map[string]string{"zone": "us-central1-a", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-b", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
				{labels: map[string]string{"zone": "us-central1-c", "machine_type": "n1-standard-4", "location_policy": "BALANCED"}},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:                          newTestDistribution(allZones, []int64{0, 32, 0}),
				{machineType: "n1-standard-16", locationPolicy: "LOCATION_POLICY_UNSPECIFIED"}: newTestDistribution(allZones, []int64{0, 0, 48}),
			},
		},
		{
			name: "target size error, node distribution not updated",
			oldCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:      newTestDistribution(allZones, []int64{0, 0, 0}),
			},
			getTargetSizeErr: true,
			migConfigs: []testMigConfig{
				{zone: "us-central1-b", machineType: "n1-standard-8", locationPolicy: gke.LocationPolicyAny, targetSize: 4},
				{zone: "us-central1-c", machineType: "n1-standard-16", locationPolicy: gke.LocationPolicyUnspecified, targetSize: 3},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:      newTestDistribution(allZones, []int64{0, 0, 0}),
			},
		},
		{
			name: "node info missing, node distribution not updated",
			oldCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:      newTestDistribution(allZones, []int64{0, 0, 0}),
			},
			nodeInfoMissing: true,
			migConfigs: []testMigConfig{
				{zone: "us-central1-b", machineType: "n1-standard-8", locationPolicy: gke.LocationPolicyAny, targetSize: 4},
				{zone: "us-central1-c", machineType: "n1-standard-16", locationPolicy: gke.LocationPolicyUnspecified, targetSize: 3},
			},
			wantCoreDistribution: map[distributionConfig]distribution{
				{machineType: "n1-standard-4", locationPolicy: "BALANCED"}: newTestDistribution(allZones, []int64{40, 0, 0}),
				{machineType: "n1-standard-8", locationPolicy: "ANY"}:      newTestDistribution(allZones, []int64{0, 0, 0}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := &gke.GkeManagerMock{}
			manager.On("GetZonesInRegion", "us-central1").Return(allZones, nil).Once()
			manager.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.E2)
			provider, err := gke.BuildGkeCloudProvider(manager, nil, nil, true, "us-central1", nil, false, false, nil, "", nil, nil, nil, 1000)
			if err != nil {
				t.Fatalf("gke.BuildGkeCloudProvider() error = %v", err)
			}

			var migs []*gke.GkeMig
			nodeInfos := make(map[string]*framework.NodeInfo)
			for i, config := range tc.migConfigs {
				config.name = fmt.Sprintf("test-mig-%v", i)
				mig := newTestMig(manager, config, tc.getTargetSizeErr)
				migs = append(migs, mig)

				if !tc.nodeInfoMissing {
					nodeInfos[mig.Id()] = getNodeInfo(t, config)
				}
			}
			manager.On("GetGkeMigs").Return(migs).Once()

			observer := &mockCoreDistributionObserver{}
			for _, updateCall := range tc.wantUpdateMetricCalls {
				observer.On("UpdateNodeZonalDistribution", updateCall.labels, updateCall.count).Once()
			}
			for _, unregisterCall := range tc.wantUnregisterMetricCalls {
				observer.On("UnregisterNodeZonalDistribution", unregisterCall.labels).Once()
			}

			metrics := &coreDistributionMetrics{observer: observer, coreDistribution: tc.oldCoreDistribution}
			metrics.updateCoreDistribution(&context.AutoscalingContext{CloudProvider: provider}, nodeInfos)
			if diff := cmp.Diff(tc.wantCoreDistribution, metrics.coreDistribution, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Processor.BalanceScaleUpBetweenGroups() diff (-want +got):\n%s", diff)
			}

			observer.AssertExpectations(t)
		})
	}
}

type mockCoreDistributionObserver struct {
	mock.Mock
}

func (o *mockCoreDistributionObserver) UpdateNodeZonalDistribution(labels map[string]string, count int64) {
	o.Called(labels, count)
}

func (o *mockCoreDistributionObserver) UnregisterNodeZonalDistribution(labels map[string]string) {
	o.Called(labels)
}

func newTestDistribution(zones []string, count []int64) distribution {
	dist := make(distribution)
	for i, zone := range zones {
		dist[zone] = count[i]
	}
	return dist
}

type testMigConfig struct {
	name           string
	zone           string
	machineType    string
	locationPolicy gke.LocationPolicyEnum
	targetSize     int
}

func newTestMig(mockManager *gke.GkeManagerMock, config testMigConfig, getSizeErr bool) *gke.GkeMig {
	mig := gke.NewTestGkeMigBuilder().
		SetGkeManager(mockManager).
		SetGceRef(gce.GceRef{Project: "test-project", Zone: config.zone, Name: config.name}).
		SetSpec(&gkeclient.NodePoolSpec{MachineType: config.machineType}).
		SetLocationPolicy(config.locationPolicy).
		SetExist(true).
		Build()

	if getSizeErr {
		mockManager.On("GetMigSize", mig).Return(int64(0), errors.New("mig size get error")).Once()
	} else {
		mockManager.On("GetMigSize", mig).Return(int64(config.targetSize), nil)
	}

	return mig
}

func getNodeInfo(t *testing.T, config testMigConfig) *framework.NodeInfo {
	machineTypeSplit := strings.Split(config.machineType, "-")
	cores, err := strconv.ParseInt(machineTypeSplit[len(machineTypeSplit)-1], 10, 64)
	assert.NoError(t, err)
	node := &apiv1.Node{
		Status: apiv1.NodeStatus{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewQuantity(cores, resource.DecimalSI),
			},
		},
	}
	return framework.NewTestNodeInfo(node)
}

type updateMetricsCall struct {
	labels map[string]string
	count  int64
}

type unregisterMetricsCall struct {
	labels map[string]string
}
