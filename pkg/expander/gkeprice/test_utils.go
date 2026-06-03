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

package gkeprice

import (
	gce_api "google.golang.org/api/compute/v1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
)

type staticClusterAnalyzer struct {
	preferredCpuCount int64
}

// NewStaticClusterAnalyzer returns staticClusterAnalyzer with preferred node set
func NewStaticClusterAnalyzer(preferredCpuCount int64) ClusterAnalyzer {
	return &staticClusterAnalyzer{
		preferredCpuCount: preferredCpuCount,
	}
}

// Analyze return staticClusterAnalysis with preferred node set
func (ca *staticClusterAnalyzer) Analyze(map[string]*framework.NodeInfo) (ClusterAnalysis, error) {
	return &staticClusterAnalysis{
		preferredCpuCount: ca.preferredCpuCount,
	}, nil
}

func (ca *staticClusterAnalyzer) AnalyzeUserWorkloadUse() (UserWorkloadClusterAnalysis, error) {
	return &staticClusterAnalysis{}, nil
}

// staticClusterAnalysis behaves similarly to OSS price expander
type staticClusterAnalysis struct {
	preferredCpuCount int64
	err               error
}

// GetPreferredCpuCount returns preferred node based on the cluster size
func (cs *staticClusterAnalysis) GetPreferredCpuCount(expander.Option, *framework.NodeInfo) (int64, error) {
	return cs.preferredCpuCount, cs.err
}

// GetReusableResources returns no reusable resources
func (cs *staticClusterAnalysis) GetReusableResources(expander.Option, *framework.NodeInfo) (*apiv1.Pod, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (cs *staticClusterAnalysis) GetPodResourceRequestApproximation([]framework.NodeInfo) (Resource, error) {
	return Resource{}, cloudprovider.ErrNotImplemented
}

type staticGroupCountReducer struct {
}

// NewLegacyGroupCountReducer returns GroupCountReducer with previous behavior.
func NewLegacyGroupCountReducer() GroupCountReducer {
	return &staticGroupCountReducer{}
}

// GroupCreationPenalty returns penalty for creation of a new node group.
func (pcr *staticGroupCountReducer) GroupCreationPenalty(hasGpu bool) float64 {
	return 1.5
}

func (pcr *staticGroupCountReducer) BaseGroupCreationPenalty() float64 {
	return 1.0004
}

type testMachineTypeBalancer struct {
}

// NewTestMachineTypeBalancer returns MachineTypeBalaner returning neutral balancing factor.
func NewTestMachineTypeBalancer() MachineTypeBalancer {
	return &testMachineTypeBalancer{}
}

// MachineTypeBalancingFactor returns the balancing factor for the given machine family
func (b *testMachineTypeBalancer) MachineTypeBalancingFactor(_ string, _ map[string]*framework.NodeInfo) float64 {
	return 1.0
}

type staticRelaxedGroupPenaltyChecker struct {
	enabled bool
}

// StaticRelaxedGroupPenaltyChecker returns a checker which decides whether
// relaxed group penalty should be used when scoring scale-up options.
func NewStaticRelaxedGroupPenaltyChecker(enabled bool) *staticRelaxedGroupPenaltyChecker {
	return &staticRelaxedGroupPenaltyChecker{enabled: enabled}
}

func (tc *staticRelaxedGroupPenaltyChecker) Enabled() bool {
	return tc.enabled
}

// NewTestStrategy returns a test expansion strategy
func NewTestStrategy(cloudProvider provider.GkeExpanderCloudProvider, pricingModel cloudprovider.PricingModel, preferredCpuCount int64, opts ...func(*gkePriceBased)) expander.Strategy {
	s := &gkePriceBased{
		pricingModel:                   pricingModel,
		clusterAnalyzer:                NewStaticClusterAnalyzer(preferredCpuCount),
		groupCountReducer:              NewLegacyGroupCountReducer(),
		relaxedNodeGroupPenaltyChecker: NewStaticRelaxedGroupPenaltyChecker(false),
		machineTypeBalancer:            NewTestMachineTypeBalancer(),
		localSSDDiskSizeProvider:       localssdsize.NewSimpleLocalSSDProvider(),
		upcomingChecker:                &asyncnodegroups.MockAsyncNodeGroupStateChecker{IsUpcomingNodeGroup: map[string]bool{}},
		cloudProvider:                  cloudProvider,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithPvmUnfitnessPenalty enables pvmUnfitnessPenalty.
func WithPvmUnfitnessPenalty() func(*gkePriceBased) {
	return func(g *gkePriceBased) {
		g.pvmUnfitnessPenaltyEnabled = true
	}
}

// WithReservations sets GCE reservations for testing.
func WithReservations(gceReservations []*gce_api.Reservation) func(*gkePriceBased) {
	return func(g *gkePriceBased) {
		mGceClient := gceclient.BuildAutoscalingInternalGceClientMock().
			WithFetchZones(func(region string) ([]string, error) { return []string{"us-central1-a"}, nil })
		puller := gceclient.NewReservationsPuller(mGceClient, nil, nil, "", false, "us-central1")
		puller.SetReservations(gceReservations)
		g.reservationsPuller = puller
	}
}

// WithUpcomingChecker overrides upcoming checker
func WithUpcomingChecker(uc asyncnodegroups.AsyncNodeGroupStateChecker) func(*gkePriceBased) {
	return func(g *gkePriceBased) {
		g.upcomingChecker = uc
	}
}

// WithRelaxedGroupPenaltyChecker overrides relaxed node group penalty checker.
func WithRelaxedGroupPenaltyChecker(checker RelaxedNodeGroupPenaltyChecker) func(*gkePriceBased) {
	return func(g *gkePriceBased) {
		g.relaxedNodeGroupPenaltyChecker = checker
	}
}
