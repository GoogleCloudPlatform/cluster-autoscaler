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
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	crdutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	listerutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	crdRules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

var registerMetricsOnce sync.Once

type testFixture struct {
	pod          *v1.Pod
	crdNoRules   crd.CRD
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

	crdNoRules := crdutils.NewTestCrd(
		crdutils.WithName("test-ccc"),
		crdutils.WithLabel(gkelabels.ComputeClassLabel),
	)

	crdRuleFleet := crdutils.NewTestCrd(
		crdutils.WithName("test-ccc"),
		crdutils.WithLabel(gkelabels.ComputeClassLabel),
		crdutils.WithRules([]crdRules.Rule{
			crdRules.NewRule(crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyFleetEfficiency))),
		}),
	)

	crdRuleCost := crdutils.NewTestCrd(
		crdutils.WithName("test-ccc"),
		crdutils.WithLabel(gkelabels.ComputeClassLabel),
		crdutils.WithRules([]crdRules.Rule{
			crdRules.NewRule(crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyLowestCost))),
		}),
	)

	ngFleet1 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe1").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build()
	ngFleet2 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe2").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-2"}).Build()

	optFleet1 := expander.Option{
		NodeGroup: ngFleet1,
		Pods:      []*v1.Pod{pod},
	}
	optFleet2 := expander.Option{
		NodeGroup: ngFleet2,
		Pods:      []*v1.Pod{pod},
	}

	ngAverage1 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-avg-1").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	ngAverage2 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-avg-2").SetGceRefZone("us-central1-b").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()

	optAverage := expander.Option{
		NodeGroup:         ngAverage1,
		SimilarNodeGroups: []cloudprovider.NodeGroup{ngAverage2},
		Pods:              []*v1.Pod{pod},
	}

	ngOther := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-other").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build()

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
		crdNoRules:        crdNoRules,
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
	name                      string
	crds                      []crd.CRD
	options                   []expander.Option
	nodeInfos                 map[string]*framework.NodeInfo
	flexAdvisorSetup          func(*instanceavailability.MockProvider)
	expectedBestOptions       []expander.Option
	expectedErrorLog          string
	reservations              []*gce_api.Reservation
	clusterDefaultStrategy    options.ClusterDefaultAllocationStrategy
	stringExperimentValues    map[string]string
	boolExperimentValues      map[string]bool
	autoprovisioningLocations []string
	trimmedLocations          []string
	backoff                   base_backoff.Backoff
}

// fakeBackoff reports uncreated node groups as backed off by machine type and zone (mirroring zone-scoped
// NAP / resource-based backoff).
type fakeBackoff struct {
	backedOffUncreated map[string]bool
}

func newFakeBackoff() *fakeBackoff {
	return &fakeBackoff{backedOffUncreated: map[string]bool{}}
}

func (b *fakeBackoff) withBackedOffUncreated(machineType, zone string) *fakeBackoff {
	b.backedOffUncreated[machineType+"/"+zone] = true
	return b
}

func (b *fakeBackoff) Backoff(cloudprovider.NodeGroup, *framework.NodeInfo, cloudprovider.InstanceErrorInfo, time.Time) time.Time {
	return time.Time{}
}

func (b *fakeBackoff) BackoffStatus(ng cloudprovider.NodeGroup, _ *framework.NodeInfo, _ time.Time) base_backoff.Status {
	if gkeNg, ok := ng.(gke.NodeGroup); ok && !ng.Exist(context.TODO()) {
		return base_backoff.Status{IsBackedOff: b.backedOffUncreated[gkeNg.MachineType()+"/"+gkeNg.GceRef().Zone]}
	}
	return base_backoff.Status{}
}

func (b *fakeBackoff) RemoveBackoff(cloudprovider.NodeGroup, *framework.NodeInfo) {}

func (b *fakeBackoff) RemoveStaleBackoffData(time.Time) {}

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
		cpBuilder := gke.NewTestAutoprovisioningCloudProviderBuilder().
			WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil))
		if len(tc.autoprovisioningLocations) > 0 {
			cpBuilder = cpBuilder.WithAutoprovisioningLocations(tc.autoprovisioningLocations...)
		}
		if tc.trimmedLocations != nil {
			cpBuilder = cpBuilder.WithTrimmedLocations(tc.trimmedLocations)
		}
		cloudProvider := cpBuilder.Build()
		localSSDDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()

		var puller *gceclient.ReservationsPuller
		if len(tc.reservations) > 0 {
			mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
				WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a", "us-central1-b"}, nil })
			puller, _ = gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
			puller.SetReservations(tc.reservations)
		}

		var em experiments.Manager
		if tc.stringExperimentValues != nil || tc.boolExperimentValues != nil {
			em = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolExperimentValues, tc.stringExperimentValues)
		} else {
			em = experiments.NewMockManager()
		}

		filter := NewFilter(flexAdvisor, lister, puller, nil, cloudProvider, localSSDDiskSizeProvider, tc.clusterDefaultStrategy, gceFlexAdvisorEnabled, em, tc.backoff)

		nodeInfos := tc.nodeInfos
		if nodeInfos == nil {
			nodeInfos = map[string]*framework.NodeInfo{}
		}

		var logBuf strings.Builder
		klog.SetOutput(&logBuf)
		klog.LogToStderr(false)
		defer klog.LogToStderr(true)

		gotOptions := filter.BestOptions(context.TODO(), tc.options, nodeInfos)
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
		instanceavailability.NewSnapshot(m, "test-ccc", machineType, "guidance", "", nil, scores),
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
			name:                "CCC without strategies, cluster default not specified - doesnt use FA, returns original options",
			crds:                []crd.CRD{f.crdNoRules},
			options:             []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:    flexAdvisorNotCalledSetup,
			expectedBestOptions: []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                   "CCC without strategies, cluster default is lowest cost - doesnt use FA, returns original options",
			crds:                   []crd.CRD{f.crdNoRules},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyLowestCost,
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                   "CCC without strategies, cluster default is fleet efficiency - calls FA, scores the options",
			crds:                   []crd.CRD{f.crdNoRules},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       defaultFlexAdvisorSetup,
			expectedBestOptions:    []expander.Option{f.optFleet2},
		},
		{
			name:                   "CCC without strategies, cluster default not specified, experiment is lowest cost - doesnt use FA, returns original options",
			crds:                   []crd.CRD{f.crdNoRules},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "lowest-cost"},
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                   "CCC without strategies, cluster default not specified, experiment is fleet efficiency - calls FA, scores the options",
			crds:                   []crd.CRD{f.crdNoRules},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       defaultFlexAdvisorSetup,
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			expectedBestOptions:    []expander.Option{f.optFleet2},
		},
		{
			name:                   "CCC with strategy=lowest-cost - doesnt use FA, returns original options",
			crds:                   []crd.CRD{f.crdRuleCost},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                   "CCC with strategy=fleet-efficiency - calls FA, scores the options",
			crds:                   []crd.CRD{f.crdRuleFleet},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "lowest-cost"},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyLowestCost,
			flexAdvisorSetup:       defaultFlexAdvisorSetup,
			expectedBestOptions:    []expander.Option{f.optFleet2},
		},
		{
			name:                   "CCC without strategies, cluster default is fleet efficiency, default strategy disabled by experiment - doesn't use FA, returns original options",
			crds:                   []crd.CRD{f.crdNoRules},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			boolExperimentValues:   map[string]bool{experiments.DefaultAllocationStrategyEnabledFlag: false},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                   "CCC without strategies, experiment default is fleet efficiency, default strategy below min CA version - doesn't use FA, returns original options",
			crds:                   []crd.CRD{f.crdNoRules},
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			boolExperimentValues:   map[string]bool{experiments.DefaultAllocationStrategyMinCAVersionFlag: false},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name:                 "CCC with strategy=fleet-efficiency, default strategy disabled by experiment - calls FA, scores the options",
			crds:                 []crd.CRD{f.crdRuleFleet},
			boolExperimentValues: map[string]bool{experiments.DefaultAllocationStrategyEnabledFlag: false},
			options:              []expander.Option{f.optFleet1, f.optFleet2},
			flexAdvisorSetup:     defaultFlexAdvisorSetup,
			expectedBestOptions:  []expander.Option{f.optFleet2},
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

	ngNap := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-2"}).Build()
	optNap := expander.Option{
		NodeGroup: ngNap,
		Pods:      []*v1.Pod{f.pod},
	}

	ngExisting := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-existing").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build()
	optExisting := expander.Option{
		NodeGroup: ngExisting,
		Pods:      []*v1.Pod{f.pod},
	}

	ngNapWithLocations := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-loc").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "n2-standard-2",
		Locations:   []string{"us-central1-a", "us-central1-b"},
	}).Build()
	optNapWithLocations := expander.Option{
		NodeGroup: ngNapWithLocations,
		Pods:      []*v1.Pod{f.pod},
	}

	ngNapCompact := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-compact").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
		MachineType:    "n2-standard-2",
		PlacementGroup: placement.Spec{Policy: "COMPACT"},
	}).Build()
	optNapCompact := expander.Option{
		NodeGroup: ngNapCompact,
		Pods:      []*v1.Pod{f.pod},
	}

	ngNapSpecificRes := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-res").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "n2-standard-2",
		ReservationAffinity: &gke_api_beta.ReservationAffinity{
			ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
		},
	}).Build()
	optNapSpecificRes := expander.Option{
		NodeGroup: ngNapSpecificRes,
		Pods:      []*v1.Pod{f.pod},
	}

	ngExistingRegional1 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-reg").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	ngExistingRegional2 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-reg").SetGceRefZone("us-central1-b").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	ngExistingRegional3 := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-reg").SetGceRefZone("us-central1-c").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	optExistingRegional := expander.Option{
		NodeGroup:         ngExistingRegional1,
		SimilarNodeGroups: []cloudprovider.NodeGroup{ngExistingRegional2, ngExistingRegional3},
		Pods:              []*v1.Pod{f.pod},
	}

	ngNapRegional := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-reg").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	optNapRegional := expander.Option{
		NodeGroup: ngNapRegional,
		Pods:      []*v1.Pod{f.pod},
	}

	// Zonal-by-design node pools with the same hardware as ngNapRegional.
	ngZonalA := gke.NewTestGkeMigBuilder().SetId("pool-a").SetNodePoolName("pool-a").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	ngZonalB := gke.NewTestGkeMigBuilder().SetId("pool-b").SetNodePoolName("pool-b").SetGceRefZone("us-central1-b").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build()
	optZonalA := expander.Option{
		NodeGroup: ngZonalA,
		Pods:      []*v1.Pod{f.pod},
	}
	optZonalB := expander.Option{
		NodeGroup: ngZonalB,
		Pods:      []*v1.Pod{f.pod},
	}

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
		{
			name:                      "Uncreated NAP candidate in regional cluster averages score across all autoprovisioning locations",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options:                   []expander.Option{optNap, optExisting},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				// NAP n2-standard-2 average across 3 zones: (0.9 + 0.3 + 0.3)/3 = 0.5
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				})
				// Existing pool single-zone score: 0.6 > 0.5, so optExisting wins
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{
					"us-central1-a": 0.6,
				})
			},
			expectedBestOptions: []expander.Option{optExisting},
		},
		{
			name:                      "Uncreated NAP candidate with CCC spec.Locations scores only across specified locations",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options:                   []expander.Option{optNapWithLocations, optExisting},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				// Locations specified as [us-central1-a, us-central1-b], so us-central1-c is excluded.
				// Average across specified locations: (0.8 + 0.6)/2 = 0.7 (whereas across all 3 zones it would be 0.5)
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{
					"us-central1-a": 0.8,
					"us-central1-b": 0.6,
					"us-central1-c": 0.1,
				})
				// Existing pool single-zone score: 0.65 < 0.7, so optNapWithLocations wins
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{
					"us-central1-a": 0.65,
				})
			},
			expectedBestOptions: []expander.Option{optNapWithLocations},
		},
		{
			name:                      "Uncreated NAP candidate with single-zone compact placement falls back to lowest cost",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options:                   []expander.Option{optNapCompact, optExisting},
			expectedBestOptions:       []expander.Option{optNapCompact, optExisting},
		},
		{
			name:                      "Uncreated NAP candidate with specific reservation constraint falls back to lowest cost",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options:                   []expander.Option{optNapSpecificRes, optExisting},
			expectedBestOptions:       []expander.Option{optNapSpecificRes, optExisting},
		},
		{
			name:                      "Existing regional node pool vs uncreated NAP candidate with same machine type tie",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options:                   []expander.Option{optExistingRegional, optNapRegional},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				// Both options have machineType "n2-standard-4", so GetInstanceAvailability is called twice.
				// Both receive identical regional average score (0.9 + 0.3 + 0.3)/3 = 0.5.
				scores := map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				}
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
			},
			expectedBestOptions: []expander.Option{optExistingRegional, optNapRegional},
		},
		{
			name:                      "Uncreated NAP candidate in regional cluster scores across trimmed locations when machine config is restricted",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			trimmedLocations:          []string{"us-central1-a", "us-central1-b"},
			options:                   []expander.Option{optNap, optExisting},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				// Trimmed to [us-central1-a, us-central1-b], so average is (0.8 + 0.6)/2 = 0.7 (excluding us-central1-c=0.1).
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{
					"us-central1-a": 0.8,
					"us-central1-b": 0.6,
					"us-central1-c": 0.1,
				})
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{
					"us-central1-a": 0.65,
				})
			},
			expectedBestOptions: []expander.Option{optNap},
		},
		{
			name:                      "Uncreated NAP candidate with CCC spec.Locations scores across trimmed locations when machine config is restricted in one zone",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			trimmedLocations:          []string{"us-central1-a", "us-central1-b"},
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().
						SetNodePoolName("pool-nap-loc-3").
						SetGceRefZone("us-central1-a").
						SetExist(false).
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "n2-standard-2",
							Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-c"},
						}).
						Build(),
					Pods: []*v1.Pod{f.pod},
				},
				optExisting,
			},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				// If properly trimmed to [us-central1-a, us-central1-b], average is (0.5 + 0.5)/2 = 0.5 < 0.65, so optExisting wins.
				// If NOT trimmed, average includes unsupported us-central1-c (0.99) -> (0.5 + 0.5 + 0.99)/3 = 0.663 > 0.65, wrongly choosing NAP candidate.
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{
					"us-central1-a": 0.5,
					"us-central1-b": 0.5,
					"us-central1-c": 0.99,
				})
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{
					"us-central1-a": 0.65,
				})
			},
			expectedBestOptions: []expander.Option{optExisting},
		},
		{
			name:                      "Uncreated candidate backed off in a zone excludes that zone",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			backoff:                   newFakeBackoff().withBackedOffUncreated("n2-standard-4", "us-central1-a"),
			options:                   []expander.Option{optNapRegional, optExisting},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n2-standard-4", map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				})
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{
					"us-central1-a": 0.35,
				})
			},
			// Candidate is scored across [us-central1-b, us-central1-c] (0.3) < 0.35.
			expectedBestOptions: []expander.Option{optExisting},
		},
		{
			name:                      "Existing regional node pool with different provisioning mode (Spot) does not restrict candidate zones",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().
						SetNodePoolName("pool-spot").
						SetGceRefZone("us-central1-b").
						SetExist(true).
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "n2-standard-4",
							Spot:        true,
							Locations:   []string{"us-central1-a", "us-central1-b", "us-central1-c"},
						}).
						Build(),
					Pods: []*v1.Pod{f.pod},
				},
				optNapRegional,
			},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				scores := map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				}
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
			},
			// pool-spot is only in us-central1-b without SimilarNodeGroups (score 0.3).
			// optNapRegional has Spot: false, so it is NOT restricted to [us-central1-b].
			// optNapRegional scores across all 3 zones: (0.9+0.3+0.3)/3 = 0.5 > 0.3, so optNapRegional wins!
			expectedBestOptions: []expander.Option{optNapRegional},
		},
		{
			name:                      "Existing regional node pool with different accelerators (GPU) does not restrict candidate zones",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().
						SetNodePoolName("pool-gpu").
						SetGceRefZone("us-central1-b").
						SetExist(true).
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "n2-standard-4",
							Accelerators: []*gke_api_beta.AcceleratorConfig{
								{AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 1},
							},
							Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
						}).
						Build(),
					Pods: []*v1.Pod{f.pod},
				},
				optNapRegional,
			},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				scores := map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				}
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
			},
			// pool-gpu has a GPU, while optNapRegional does not.
			// optNapRegional is NOT restricted to [us-central1-b] and scores across all 3 zones (0.5 > 0.3).
			expectedBestOptions: []expander.Option{optNapRegional},
		},
		{
			name:                      "Existing regional node pool with different Local SSD count does not restrict candidate zones",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().
						SetNodePoolName("pool-lssd").
						SetGceRefZone("us-central1-b").
						SetExist(true).
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "n2-standard-4",
							LocalSSDConfig: &gkeclient.LocalSSDConfig{
								LocalSsdCount: 1,
							},
							Locations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
						}).
						Build(),
					Pods: []*v1.Pod{f.pod},
				},
				optNapRegional,
			},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				scores := map[string]float64{
					"us-central1-a": 0.9,
					"us-central1-b": 0.3,
					"us-central1-c": 0.3,
				}
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
			},
			// pool-lssd has LocalSsdCount=1, while optNapRegional has 0.
			// optNapRegional is NOT restricted to [us-central1-b] and scores across all 3 zones (0.5 > 0.3).
			expectedBestOptions: []expander.Option{optNapRegional},
		},
		{
			name:                      "Healthy zonal-by-design same-hardware node pools do not restrict uncreated candidate zones",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			backoff:                   newFakeBackoff(),
			options:                   []expander.Option{optZonalA, optZonalB, optNapRegional},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				scores := map[string]float64{
					"us-central1-a": 0.8,
					"us-central1-b": 0.8,
					"us-central1-c": 0.2,
				}
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
				setupMockSnapshot(m, "n2-standard-4", scores)
			},
			// pool-a and pool-b are zonal by design and not backed off, so the candidate keeps all of its
			// target zones: (0.8 + 0.8 + 0.2)/3 = 0.6 < 0.8. pool-a and pool-b tie.
			expectedBestOptions: []expander.Option{optZonalA, optZonalB},
		},
		{
			name:                      "Uncreated candidate backed off in all target zones scores 0 and loses to healthy option",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b"},
			backoff:                   newFakeBackoff().withBackedOffUncreated("n2-standard-2", "us-central1-a").withBackedOffUncreated("n2-standard-2", "us-central1-b"),
			options:                   []expander.Option{optNap, optExisting},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{"us-central1-a": 0.9, "us-central1-b": 0.9})
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.6})
			},
			expectedBestOptions: []expander.Option{optExisting},
		},
		{
			name:                      "Soft ScheduleAnyway zonal topology spread does not trigger fallback and scores with fleet efficiency",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a"},
			options: []expander.Option{
				{
					NodeGroup: ngExisting,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-soft-spread"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
								},
								TopologySpreadConstraints: []v1.TopologySpreadConstraint{
									{
										MaxSkew:           1,
										TopologyKey:       v1.LabelTopologyZone,
										WhenUnsatisfiable: v1.ScheduleAnyway,
									},
								},
							},
						},
					},
				},
			},
			flexAdvisorSetup: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.8})
			},
			expectedBestOptions: []expander.Option{
				{
					NodeGroup: ngExisting,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-soft-spread"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
								},
								TopologySpreadConstraints: []v1.TopologySpreadConstraint{
									{
										MaxSkew:           1,
										TopologyKey:       v1.LabelTopologyZone,
										WhenUnsatisfiable: v1.ScheduleAnyway,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:                      "Zonal pod with topology.gke.io/zone triggers fallback to lowest cost",
			crds:                      []crd.CRD{f.crdRuleFleet},
			autoprovisioningLocations: []string{"us-central1-a"},
			options: []expander.Option{
				{
					NodeGroup: ngExisting,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-gke-zonal"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
									"topology.gke.io/zone":           "us-central1-a",
								},
							},
						},
					},
				},
			},
			flexAdvisorSetup: flexAdvisorNotCalledSetup,
			expectedBestOptions: []expander.Option{
				{
					NodeGroup: ngExisting,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-gke-zonal"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
									"topology.gke.io/zone":           "us-central1-a",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}

type firstOptionFallback struct{}

func (f *firstOptionFallback) BestOption(ctx context.Context, options []expander.Option, nodeInfo map[string]*framework.NodeInfo) *expander.Option {
	if len(options) == 0 {
		return nil
	}
	return &options[0]
}

func TestFleetEfficiencyMetrics(t *testing.T) {
	registerMetricsOnce.Do(metrics.RegisterAll)

	f := newTestFixture()

	// Custom fallback that returns the first option, so metrics are recorded.
	fallback := &firstOptionFallback{}

	ngTpu := gke.NewTestGkeMigBuilder().SetNodePoolName("pool-tpu").SetGceRefZone("us-central1-a").SetSpec(&gkeclient.NodePoolSpec{MachineType: "ct3-hightpu-4t", TpuType: "v3"}).Build()
	optTpu := expander.Option{
		NodeGroup: ngTpu,
		Pods:      []*v1.Pod{f.pod},
		NodeCount: 1,
	}

	// Clone standard options and set NodeCount to 1 for metric testing
	optFleet1 := f.optFleet1
	optFleet1.NodeCount = 1

	optFleet2 := f.optFleet2
	optFleet2.NodeCount = 1

	tests := []struct {
		name                      string
		crd                       crd.CRD
		options                   []expander.Option
		setupMock                 func(*instanceavailability.MockProvider)
		reservations              []*gce_api.Reservation
		autoprovisioningLocations []string
		backoff                   base_backoff.Backoff
		expectedRequestedStrategy cccv1.AllocationStrategy
		expectedReason            metrics.AllocationStrategyFallbackReason
		expectedMachineType       string
	}{
		{
			name:                      "Fallback - FlexAdvisorNotSupported (TPU)",
			options:                   []expander.Option{optTpu},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackFlexAdvisorNotSupported,
			expectedMachineType:       "ct3-hightpu-4t",
		},
		{
			name: "Fallback - Unsupported (Zonal Pod)",
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe1").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build(),
					NodeCount: 1,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-zonal"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
									v1.LabelTopologyZone:             "us-central1-a",
								},
							},
						},
					},
				},
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackUnsupported,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name: "Fallback - Unsupported (Zonal Pod in second option)",
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe1").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build(),
					NodeCount: 1,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-unconstrained"},
							Spec:       v1.PodSpec{},
						},
					},
				},
				{
					NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("pool-fe2").SetGceRefZone("us-central1-a").SetExist(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build(),
					NodeCount: 1,
					Pods: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "pod-zonal"},
							Spec: v1.PodSpec{
								NodeSelector: map[string]string{
									"cloud.google.com/compute-class": "test-ccc",
									v1.LabelTopologyZone:             "us-central1-a",
								},
							},
						},
					},
				},
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackUnsupported,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name: "Fallback - TieBreak (all uncreated candidates backed off in all target zones)",
			options: []expander.Option{
				{
					NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-1").SetGceRefZone("us-central1-a").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-1"}).Build(),
					NodeCount: 1,
					Pods:      []*v1.Pod{f.pod},
				},
				{
					NodeGroup: gke.NewTestGkeMigBuilder().SetNodePoolName("pool-nap-2").SetGceRefZone("us-central1-b").SetExist(false).SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-2"}).Build(),
					NodeCount: 1,
					Pods:      []*v1.Pod{f.pod},
				},
			},
			autoprovisioningLocations: []string{"us-central1-a", "us-central1-b"},
			backoff:                   newFakeBackoff().withBackedOffUncreated("n1-standard-1", "us-central1-a").withBackedOffUncreated("n1-standard-1", "us-central1-b").withBackedOffUncreated("n2-standard-2", "us-central1-a").withBackedOffUncreated("n2-standard-2", "us-central1-b"),
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.9, "us-central1-b": 0.9})
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{"us-central1-a": 0.9, "us-central1-b": 0.9})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackTieBreak,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Fallback - MissingScore (Snapshot not found)",
			options: []expander.Option{optFleet1},
			setupMock: func(m *instanceavailability.MockProvider) {
				m.On("GetInstanceAvailability", mock.Anything, mock.Anything).Return((*instanceavailability.Snapshot)(nil)).Once()
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackMissingScore,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Fallback - MissingScore (Preference score not present)",
			options: []expander.Option{optFleet1},
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackMissingScore,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Fallback - Error (Invalid score < 0)",
			options: []expander.Option{optFleet1},
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": -0.5})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackError,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Fallback - Error (Invalid score > 1)",
			options: []expander.Option{optFleet1},
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 1.5})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackError,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:                      "Lowest Cost Strategy",
			crd:                       f.crdRuleCost,
			options:                   []expander.Option{optFleet1},
			expectedRequestedStrategy: cccv1.AllocationStrategyLowestCost,
			expectedReason:            metrics.AllocationStrategyFallbackNone,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Usable Reservations",
			options: []expander.Option{optFleet1},
			reservations: []*gce_api.Reservation{
				reservations.BuildMultipleMachineReservationWithId(1, 0, 5, "n1-standard-1", "us-central1-a"),
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackReservationPresent,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Tie Break",
			options: []expander.Option{optFleet1, optFleet2},
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.5})
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{"us-central1-a": 0.5})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackTieBreak,
			expectedMachineType:       "n1-standard-1",
		},
		{
			name:    "Normal Code Path (Success)",
			options: []expander.Option{optFleet1, optFleet2},
			setupMock: func(m *instanceavailability.MockProvider) {
				setupMockSnapshot(m, "n1-standard-1", map[string]float64{"us-central1-a": 0.9})
				setupMockSnapshot(m, "n2-standard-2", map[string]float64{"us-central1-a": 0.1})
			},
			expectedRequestedStrategy: cccv1.AllocationStrategyFleetEfficiency,
			expectedReason:            metrics.AllocationStrategyFallbackNone,
			expectedMachineType:       "n1-standard-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ResetAllForTest()

			flexAdvisor := &instanceavailability.MockProvider{}
			if tc.setupMock != nil {
				tc.setupMock(flexAdvisor)
			}

			crdToUse := f.crdRuleFleet
			if tc.crd != nil {
				crdToUse = tc.crd
			}
			lister := listerutils.NewMockCrdListerWithLabel([]crd.CRD{crdToUse}, gkelabels.ComputeClassLabel)
			lister.SetDefaultCrdName(crdToUse.Name())

			var puller *gceclient.ReservationsPuller
			if len(tc.reservations) > 0 {
				mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
					WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a", "us-central1-b"}, nil })
				puller, _ = gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
				puller.SetReservations(tc.reservations)
			}

			cpBuilder := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil))
			if len(tc.autoprovisioningLocations) > 0 {
				cpBuilder = cpBuilder.WithAutoprovisioningLocations(tc.autoprovisioningLocations...)
			}
			cloudProvider := cpBuilder.Build()
			localSSDDiskSizeProvider := localssdsize.NewSimpleLocalSSDProvider()

			filter := NewFilter(flexAdvisor, lister, puller, fallback, cloudProvider, localSSDDiskSizeProvider, options.ClusterDefaultAllocationStrategyLowestCost, true, experiments.NewMockManager(), tc.backoff)

			// We don't care about the returned options here, just that the fallback logic was triggered and recorded metrics.
			_ = filter.BestOptions(context.TODO(), tc.options, map[string]*framework.NodeInfo{})

			// Verify metrics
			count, err := metrics.GetNodesWithAllocationStrategyCountForTest(string(tc.expectedRequestedStrategy), tc.expectedReason, tc.expectedMachineType)
			assert.NoError(t, err)
			assert.Equal(t, float64(1), count)

			flexAdvisor.AssertExpectations(t)
		})
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

func TestFleetEfficiencyFilter_FallbackPrecedenceResolution(t *testing.T) {
	f := newTestFixture()

	// Defining rules with the same priority score that mix "fleet-efficiency" and "lowest-cost"
	// is impossible in production due to CCC validation.
	// On unit test level this however makes sense for completeness, so it is covered with tests.
	tests := []fleetEfficiencyTestCase{
		{
			name: "explicit lowest-cost overrides explicit fleet-efficiency and cluster default",
			crds: []crd.CRD{
				crdutils.NewTestCrd(
					crdutils.WithName("test-ccc"),
					crdutils.WithLabel(gkelabels.ComputeClassLabel),
					crdutils.WithRules([]crdRules.Rule{
						crdRules.NewRule(
							crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyLowestCost)),
							crdRules.WithNodePoolsRule([]string{"pool-fe1"}),
						),
						crdRules.NewRule(
							crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyFleetEfficiency)),
							crdRules.WithNodePoolsRule([]string{"pool-fe2"}),
						),
						crdRules.NewRule(
							crdRules.WithNodePoolsRule([]string{"pool-other"}),
						),
					}),
				),
			},
			options:                []expander.Option{f.optFleet1, f.optFleet2, f.optOther},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2, f.optOther},
		},
		{
			name: "explicit lowest-cost on last rule overrides cluster default fleet-efficiency",
			crds: []crd.CRD{
				crdutils.NewTestCrd(
					crdutils.WithName("test-ccc"),
					crdutils.WithLabel(gkelabels.ComputeClassLabel),
					crdutils.WithRules([]crdRules.Rule{
						crdRules.NewRule(
							crdRules.WithNodePoolsRule([]string{"pool-fe1"}),
						),
						crdRules.NewRule(
							crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyLowestCost)),
							crdRules.WithNodePoolsRule([]string{"pool-fe2"}),
						),
					}),
				),
			},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			flexAdvisorSetup:       flexAdvisorNotCalledSetup,
			expectedBestOptions:    []expander.Option{f.optFleet1, f.optFleet2},
		},
		{
			name: "explicit fleet-efficiency on last rule overrides cluster default lowest-cost",
			crds: []crd.CRD{
				crdutils.NewTestCrd(
					crdutils.WithName("test-ccc"),
					crdutils.WithLabel(gkelabels.ComputeClassLabel),
					crdutils.WithRules([]crdRules.Rule{
						crdRules.NewRule(
							crdRules.WithNodePoolsRule([]string{"pool-fe1"}),
						),
						crdRules.NewRule(
							crdRules.WithAllocationStrategyRule(new(cccv1.AllocationStrategyFleetEfficiency)),
							crdRules.WithNodePoolsRule([]string{"pool-fe2"}),
						),
					}),
				),
			},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyLowestCost,
			flexAdvisorSetup:       defaultFlexAdvisorSetup,
			expectedBestOptions:    []expander.Option{f.optFleet2},
		},
		{
			name: "inherits cluster default fleet-efficiency",
			crds: []crd.CRD{
				crdutils.NewTestCrd(
					crdutils.WithName("test-ccc"),
					crdutils.WithLabel(gkelabels.ComputeClassLabel),
					crdutils.WithRules([]crdRules.Rule{
						crdRules.NewRule(
							crdRules.WithNodePoolsRule([]string{"pool-fe1"}),
						),
						crdRules.NewRule(
							crdRules.WithNodePoolsRule([]string{"pool-fe2"}),
						),
					}),
				),
			},
			options:                []expander.Option{f.optFleet1, f.optFleet2},
			clusterDefaultStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency,
			flexAdvisorSetup:       defaultFlexAdvisorSetup,
			expectedBestOptions:    []expander.Option{f.optFleet2},
		},
	}

	for _, tc := range tests {
		runFleetEfficiencyTest(t, tc)
	}
}

func TestTargetZonesForOption_NilNodeGroup(t *testing.T) {
	filter := &fleetEfficiencyFilter{}
	opt := expander.Option{
		NodeGroup: nil,
	}
	_, err := filter.targetZonesForOption(context.Background(), opt, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil node group")
}
