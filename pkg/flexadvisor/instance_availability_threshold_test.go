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
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func newTestMigWithAffinity(zone, machineType string, labels map[string]string, affinityType, affinityName string, accelerators []*gke_api_beta.AcceleratorConfig) *gke.GkeMig {
	mig := newTestMig(zone, machineType, labels, false, false, accelerators, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration)
	spec := mig.Spec()
	spec.ReservationAffinity = &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: affinityType,
		Values:                 []string{affinityName},
	}
	return mig
}

func TestNodeLimit(t *testing.T) {
	mig1 := newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration)
	rsv1 := reservations.BuildMultipleMachineReservationWithId(1, 0, 100, "n2-standard-4", "us-central1-a")
	rsv2 := reservations.BuildMultipleMachineReservationWithId(2, 100, 300, "n2-standard-4", "us-central1-b")
	rsv3 := reservations.NewTestReservationBuilder().
		WithId(3).
		WithName("a4x-rsv").
		WithZone("us-central1-c").
		WithMachineType("a4x-highgpu-4g").
		WithCounts(0, 18).
		WithSpecificReservationRequired(true).
		WithGuestAccelerators([]*compute.AcceleratorConfig{
			{
				AcceleratorType:  "nvidia-gb200",
				AcceleratorCount: 4,
			},
		}).
		Build()

	mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
		WithFetchZones(func(region string) ([]string, error) {
			return []string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil
		}).
		WithFetchReservationsInProject(func(project string) ([]*compute.Reservation, error) {
			return []*compute.Reservation{rsv1, rsv2, rsv3}, nil
		})
	puller, err := gceclient.NewReservationsPuller(mGceClient, nil, nil, "project-1", false, "us-central1")
	assert.NoError(t, err)

	stop := make(chan struct{})
	puller.Run(context.Background())
	defer close(stop)

	crd1 := ccc.NewCccCrd(&v1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scope-1",
		},
	}, "", false, nil, nil)

	testCases := []struct {
		name               string
		initialSetup       func(provider *instanceavailability.MockProvider)
		nodeGroup          cloudprovider.NodeGroup
		estimationContext  estimator.EstimationContext
		experimentsManager experiments.Manager
		want               int
	}{
		{
			name: "single node group without similar node groups",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": 100}).Build().NewSnapshot()).Once()
			},
			nodeGroup:         newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, nil, 0),
			want:              100,
		},
		{
			name: "single node group with similar node groups with no duplicates",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": 100, "us-central1-b": 200, "us-central1-c": 300}).Build().NewSnapshot()).Times(3)
			},
			nodeGroup: mig1,
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				newTestMig("us-central1-b", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			want: 600,
		},
		{
			name: "single node group with similar node groups with first node group duplicates",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": 100, "us-central1-b": 200, "us-central1-c": 300}).Build().NewSnapshot()).Times(3)
			},
			nodeGroup: mig1,
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				mig1,
				newTestMig("us-central1-b", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			want: 600,
		},
		{
			name: "no more capacity",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": -100, "us-central1-b": -200, "us-central1-c": 300}).Build().NewSnapshot()).Times(3)
			},
			nodeGroup: mig1,
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				newTestMig("us-central1-b", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			want: -1,
		},
		{
			name:              "ignore threshold",
			initialSetup:      nil,
			nodeGroup:         newTestMig("", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, nil, 0),
			want:              0,
		},
		{
			name: "account for matching reservations",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: n2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: n2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": -100, "us-central1-b": -200, "us-central1-c": 0}).Build().NewSnapshot()).Times(3)
			},
			nodeGroup: newTestMig("us-central1-a", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				newTestMig("us-central1-b", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			want: 300,
		},
		{
			name: "account for matching reservations and guidance",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: n2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: n2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-a": 0, "us-central1-b": 200, "us-central1-c": 0}).Build().NewSnapshot()).Times(3)
			},
			nodeGroup: newTestMig("us-central1-a", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				newTestMig("us-central1-b", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			want: 500,
		},
		{
			name: "missing availability for zone from FlexAdvisor",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").WithZonalInstanceCount(map[string]int{"us-central1-b": 200}).Build().NewSnapshot()).Once()
			},
			nodeGroup:         newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, nil, 0),
			want:              0,
		},
		{
			name: "account for matching reservations with subblock affinity",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: a4x-highgpu-4g, provisioningMode: STANDARD, gpuType: nvidia-gb200, gpuCount: 4").Return(api.NewTestInstanceAvailabilityBuilder("scope-1", "machineType: a4x-highgpu-4g, provisioningMode: STANDARD, gpuType: nvidia-gb200, gpuCount: 4").WithZonalInstanceCount(map[string]int{"us-central1-c": -100}).Build().NewSnapshot()).Once()
			},
			nodeGroup:         newTestMigWithAffinity("us-central1-c", "a4x-highgpu-4g", map[string]string{labels.ComputeClassLabel: "scope-1"}, gkeclient.ReservationAffinitySpecific, "projects/shared-project/reservations/a4x-rsv/reservationBlocks/block-1/reservationSubBlocks/sub-block-1", []*gke_api_beta.AcceleratorConfig{{AcceleratorType: "nvidia-gb200", AcceleratorCount: 4}}),
			estimationContext: estimator.NewEstimationContext(0, nil, 0),
			want:              18,
		},
		{
			name: "bin packer processing disabled globally - returns early",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: n2-standard-4, provisioningMode: STANDARD").Maybe().Panic("GetInstanceAvailability: should not be called")
			},
			nodeGroup: newTestMig("us-central1-a", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, []cloudprovider.NodeGroup{
				newTestMig("us-central1-b", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
				newTestMig("us-central1-c", "n2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			}, 0),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, nil),
			want: 0,
		},
		{
			name: "GetInstanceAvailability returns nil - doesn't apply limits",
			initialSetup: func(provider *instanceavailability.MockProvider) {
				provider.On("GetInstanceAvailability", "scope-1", "machineType: e2-standard-4, provisioningMode: STANDARD").Return(nil).Once()
			},
			nodeGroup:         newTestMig("us-central1-a", "e2-standard-4", map[string]string{labels.ComputeClassLabel: "scope-1"}, false, false, nil, EmptyTpuType, EmptyTpuTopology, api.EmptyMaxRunDuration),
			estimationContext: estimator.NewEstimationContext(0, nil, 0),
			want:              0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(instanceavailability.MockProvider)
			if tc.initialSetup != nil {
				tc.initialSetup(mockProvider)
			}
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1}, labels.ComputeClassLabel)
			cloudProvider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			manager := tc.experimentsManager
			if manager == nil {
				manager = experiments.NewMockManager()
			}
			threshold := NewInstanceAvailabilityThreshold(mockProvider, puller, localssdsize.NewSimpleLocalSSDProvider(), mockLister, cloudProvider, manager)
			got := threshold.NodeLimit(tc.nodeGroup, tc.estimationContext)

			assert.Equal(t, tc.want, got.Limit)
			mockProvider.AssertExpectations(t)
		})
	}
}
