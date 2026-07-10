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

package fleetefficiency

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	crdutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	listerutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	crdRules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

type testFixture struct {
	pod          *v1.Pod
	crdRuleFleet crd.CRD
	crdRuleCost  crd.CRD

	optFleet1         expander.Option
	optFleet2         expander.Option
	optAverage        expander.Option
	optOther          expander.Option
	optOtherNodeGroup expander.Option
}

func newTestFixture() *testFixture {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: v1.PodSpec{
			NodeSelector: map[string]string{
				"cloud.google.com/compute-class": "test-ccc",
			},
		},
	}

	fleetEfficiencyStrategy := cccv1.AllocationStrategyFleetEfficiency
	defaultStrategy := cccv1.AllocationStrategyLowestCost

	crdRuleFleet := crdutils.NewTestCrd(
		crdutils.WithName("test-ccc"),
		crdutils.WithLabel(gkelabels.ComputeClassLabel),
		crdutils.WithRules([]crdRules.Rule{
			crdRules.NewRule(crdRules.WithAllocationStrategyRule(&fleetEfficiencyStrategy)),
		}),
	)

	crdRuleCost := crdutils.NewTestCrd(
		crdutils.WithName("test-ccc"),
		crdutils.WithLabel(gkelabels.ComputeClassLabel),
		crdutils.WithRules([]crdRules.Rule{
			crdRules.NewRule(crdRules.WithAllocationStrategyRule(&defaultStrategy)),
		}),
	)

	ngFleet1 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe1").SetGceRefZone("us-central1-a").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build()
	ngFleet2 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe2").SetGceRefZone("us-central1-a").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-2"}).Build()

	optFleet1 := expander.Option{
		NodeGroup: ngFleet1,
		Pods:      []*v1.Pod{pod},
	}
	optFleet2 := expander.Option{
		NodeGroup: ngFleet2,
		Pods:      []*v1.Pod{pod},
	}

	ngAverage1 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-avg-1").SetGceRefZone("us-central1-a").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	ngAverage2 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-avg-2").SetGceRefZone("us-central1-b").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()

	optAverage := expander.Option{
		NodeGroup:         ngAverage1,
		SimilarNodeGroups: []cloudprovider.NodeGroup{ngAverage2},
		Pods:              []*v1.Pod{pod},
	}

	ngOther := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-other").SetGceRefZone("us-central1-a").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build()

	optOther := expander.Option{
		NodeGroup: ngOther,
		Pods:      []*v1.Pod{pod},
	}

	optOtherNodeGroup := expander.Option{
		NodeGroup: testprovider.NewTestNodeGroup("mock-other-ng", 0, 0, 0, false, false, "", nil, nil),
		Pods:      []*v1.Pod{pod},
	}

	return &testFixture{
		pod:               pod,
		crdRuleFleet:      crdRuleFleet,
		crdRuleCost:       crdRuleCost,
		optFleet1:         optFleet1,
		optFleet2:         optFleet2,
		optAverage:        optAverage,
		optOther:          optOther,
		optOtherNodeGroup: optOtherNodeGroup,
	}
}

type fleetEfficiencyTestCase struct {
	name                string
	crds                []crd.CRD
	options             []expander.Option
	nodeInfos           map[string]*framework.NodeInfo
	flexAdvisorSetup    func(*instanceavailability.MockProvider)
	expectedBestOptions []expander.Option
	expectedErrorLog    string
	reservations        []*gce_api.Reservation
}

func runFleetEfficiencyTest(t *testing.T, tc fleetEfficiencyTestCase) {
	t.Run(tc.name, func(t *testing.T) {
		flexAdvisor := &instanceavailability.MockProvider{}
		if tc.flexAdvisorSetup != nil {
			tc.flexAdvisorSetup(flexAdvisor)
		}

		lister := listerutils.NewMockCrdListerWithLabel(tc.crds, gkelabels.ComputeClassLabel)
		if len(tc.crds) > 0 {
			lister.SetDefaultCrdName(tc.crds[0].Name())
		}

		gceFlexAdvisorEnabled := true
		cloudProvider := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
			Build()
		localSSDDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()

		var puller *gceclient.ReservationsPuller
		if len(tc.reservations) > 0 {
			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a", "us-central1-b"}, nil })
			puller, _ = gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
			puller.SetReservations(tc.reservations)
		}

		filter := NewFilter(flexAdvisor, lister, puller, cloudProvider, localSSDDiskSizeProvider, gceFlexAdvisorEnabled, experiments.NewMockManager())

		nodeInfos := tc.nodeInfos
		if nodeInfos == nil {
			nodeInfos = map[string]*framework.NodeInfo{}
		}

		var logBuf strings.Builder
		klog.SetOutput(&logBuf)
		klog.LogToStderr(false)
		defer klog.LogToStderr(true)

		gotOptions := filter.BestOptions(tc.options, nodeInfos)
		assert.ElementsMatch(t, tc.expectedBestOptions, gotOptions)
		flexAdvisor.AssertExpectations(t)

		if tc.expectedErrorLog != "" {
			assert.Contains(t, logBuf.String(), tc.expectedErrorLog)
		}
	})
}

func setupMockSnapshot(m *instanceavailability.MockProvider, machineType string, scores map[string]float64) {
	m.On("GetInstanceAvailability", mock.Anything, mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, machineType)
	})).Return(
		instanceavailability.NewSnapshot(m, "test-ccc", machineType, "guidance", nil, scores),
	).Once()
}

func defaultFlexAdvisorSetup(m *instanceavailability.MockProvider) {
	setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.1})
	setupMockSnapshot(m, "n2-standard-2", map[string]float64{"us-central1-a": 0.2})
}

func flexAdvisorNotCalledSetup(m *instanceavailability.MockProvider) {
	m.On("GetInstanceAvailability", mock.Anything, mock.Anything).Maybe().Panic("flexadvisor: should not be called")
}

func TestFleetEfficiencyFilter_SelectingStrategy(t *testing.T) {
	f := newTestFixture()

	tests := []fleetEfficiencyTestCase{
		{
			name:                "CCC without strategies - doesnt use FA, returns original options",
			crds:                []crd.CRD{crdutils.NewTestCrd(crdutils.WithName("test-ccc"), crdutils.WithLabel(gkelabels.ComputeClassLabel))},
			options:             []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:    flexAdvisorNotCalledSetup,
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                "CCC with strategy=lowest-cost - doesnt use FA, returns original options",
			crds:                []crd.CRD{f.crdRuleCost},
			options:             []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:    flexAdvisorNotCalledSetup,
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                "CCC with strategy=fleet-efficiency - calls FA, scores the options",
			crds:                []crd.CRD{f.crdRuleFleet},
			options:             []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:    defaultFlexAdvisorSetup,
			expectedBestOptions: []expander.Option{f.optFleet2},
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}

func TestFleetEfficiencyFilter_Reservations(t *testing.T) {
	f := newTestFixture()

	tests := []fleetEfficiencyTestCase{
		{
			name:    "Option has matching unused reservation - doesnt use FA, returns original options",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservationWithId(1, 0, 5, "n1-standard-1", "us-central1-a"),
			},
			flexAdvisorSetup:    flexAdvisorNotCalledSetup,
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:    "Reservations exist but do not match options - calls FA, scores the options",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservationWithId(1, 0, 5, "some-other-machine", "us-central1-a"),
			},
			flexAdvisorSetup:    defaultFlexAdvisorSetup,
			expectedBestOptions: []expander.Option{f.optFleet2},
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}

func TestFleetEfficiencyFilter_Scoring(t *testing.T) {
	f := newTestFixture()

	tests := []fleetEfficiencyTestCase{
		{
			name:    "Averaged score across zones - fleet efficiency",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optAverage, f.optOther},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n2-standard-4", map[string]float64{
					"us-central1-a": 0.2,
					"us-central1-b": 0.1, // average will be 0.15
				})
				setupMockSnapshot(m, "n1-standard-4", map[string]float64{
					"us-central1-a": 0.18, // This is higher than 0.15, so this should win
				})
			},
			expectedBestOptions: []expander.Option{f.optOther},
		},
		{
			name:    "Scores within epsilon - both returned - fleet efficiency",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optAverage, f.optOther},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n2-standard-4", map[string]float64{
					"us-central1-a": 0.1500001,
					"us-central1-b": 0.1500001,
				})
				setupMockSnapshot(m, "n1-standard-4", map[string]float64{
					"us-central1-a": 0.1500005, // difference is 4e-7 < 1e-6 (epsilon)
				})
			},
			expectedBestOptions: []expander.Option{f.optAverage, f.optOther},
		},
		{
			name:    "FleetEfficiency score fails for missing snapshot",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				var snapshot *instanceavailability.Snapshot = nil
				m.On("GetInstanceAvailability", mock.Anything, mock.Anything).Return(snapshot).Once()
			},
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:    "FleetEfficiency score fails for missing zonal score",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-b": 0.5})
			},
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:    "FleetEfficiency score fails for score below zero",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": -0.5})
			},
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:    "FleetEfficiency score fails for score above one",
			crds:    []crd.CRD{f.crdRuleFleet},
			options: []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 1.5})
			},
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}

func TestFleetEfficiencyFilter_Errors(t *testing.T) {
	f := newTestFixture()

	tests := []fleetEfficiencyTestCase{
		{
			name:                "Lister error (missing CRD)",
			crds:                nil,
			options:             []expander.Option{f.optFleet1, f.optFleet2},
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
			expectedErrorLog:    "failed to get the CRD for pod: crd doesnt exist",
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}
