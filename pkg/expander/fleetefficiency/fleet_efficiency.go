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
	"fmt"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
)

type fleetEfficiencyFilter struct {
	flexAdvisor                      instanceavailability.Provider
	cccLister                        lister.Lister
	reservationsPuller               *gceclient.ReservationsPuller
	cloudProvider                    provider.GkeExpanderCloudProvider
	localSSDDiskSizeProvider         localssdsize.LocalSSDSizeProvider
	clusterDefaultAllocationStrategy options.ClusterDefaultAllocationStrategy
	gceFlexAdvisorEnabled            bool
	experimentsManager               experiments.Manager
}

// NewFilter creates a new instance of the fleet efficiency Filter.
func NewFilter(
	flexAdvisor instanceavailability.Provider,
	cccLister lister.Lister,
	reservationsPuller *gceclient.ReservationsPuller,
	cloudProvider provider.GkeExpanderCloudProvider,
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider,
	clusterDefaultAllocationStrategy options.ClusterDefaultAllocationStrategy,
	gceFlexAdvisorEnabled bool,
	experimentsManager experiments.Manager,
) *fleetEfficiencyFilter {
	return &fleetEfficiencyFilter{
		flexAdvisor:                      flexAdvisor,
		cccLister:                        cccLister,
		reservationsPuller:               reservationsPuller,
		cloudProvider:                    cloudProvider,
		localSSDDiskSizeProvider:         localSSDDiskSizeProvider,
		clusterDefaultAllocationStrategy: clusterDefaultAllocationStrategy,
		gceFlexAdvisorEnabled:            gceFlexAdvisorEnabled,
		experimentsManager:               experimentsManager,
	}
}

func (f *fleetEfficiencyFilter) BestOptions(expansionOptions []expander.Option, nodeInfo map[string]*framework.NodeInfo) []expander.Option {
	if !IsFleetEfficiencyEnabled(f.gceFlexAdvisorEnabled, f.experimentsManager) {
		klog.V(4).Infof("FleetEfficiencyFilter: expander disabled by experiments")
		return expansionOptions
	}

	if len(expansionOptions) == 0 || len(expansionOptions[0].Pods) == 0 {
		return expansionOptions
	}

	// Verify the allocation strategy.
	samplePod := expansionOptions[0].Pods[0]
	crd, _, err := f.cccLister.PodCrd(samplePod)
	if err != nil {
		klog.Errorf("FleetEfficiencyFilter: failed to get the CRD for pod: %v", err)
		return expansionOptions
	}
	if crd == nil {
		return expansionOptions
	}
	if !f.isFleetEfficiencyStrategy(crd, expansionOptions[0]) {
		klog.V(4).Infof("FleetEfficiencyFilter: skipping, allocation strategy is not fleet-efficiency")
		return expansionOptions
	}

	if f.hasUsableReservations(expansionOptions) {
		klog.V(4).Infof("FleetEfficiencyFilter: some options have usable reservations, skipping")
		return expansionOptions
	}

	// Calculate the scores to find the best options.
	scores := make([]float64, len(expansionOptions))
	maxScore := -1.0
	for i, option := range expansionOptions {
		score, err := f.scoreOption(option)
		if err != nil {
			klog.V(4).Infof("FleetEfficiencyFilter: failed to score option %s, ignoring strategy: %v", option.NodeGroup.Id(), err)
			return expansionOptions
		}
		scores[i] = score
		if score > maxScore {
			maxScore = score
		}
	}

	const epsilon = 0.000001
	var bestOptions []expander.Option
	for i, option := range expansionOptions {
		if maxScore-scores[i] <= epsilon {
			bestOptions = append(bestOptions, option)
		}
	}

	return bestOptions
}

func (f *fleetEfficiencyFilter) scoreOption(option expander.Option) (float64, error) {
	instanceRef, err := flexadvisor.ConstructInstanceReference(option.NodeGroup, f.cccLister, f.experimentsManager)
	if err != nil {
		return 0, fmt.Errorf("failed to construct instance reference: %w", err)
	}
	snapshot := f.flexAdvisor.GetInstanceAvailability(instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
	if snapshot == nil {
		return 0, fmt.Errorf("flex advisor snapshot not found for keys: scope=%q, config=%s", instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
	}

	totalScore := 0.0
	count := 0

	processNodeGroup := func(ng cloudprovider.NodeGroup) error {
		gkeNg, ok := ng.(gke.NodeGroup)
		if !ok {
			return fmt.Errorf("node group %s is not a GKE node group", ng.Id())
		}
		zone := gkeNg.GceRef().Zone
		score, found := snapshot.GcePreferenceScore(zone)
		if !found {
			return fmt.Errorf("GCE Preference Score not present for scope %s and zone %s", instanceRef.FlexibilityScopeKey, zone)
		}
		if score < 0 || score > 1 {
			// TODO(b/527312993): Move the filtering to flex advisor (reject invalid scores).
			return fmt.Errorf("invalid GCE Preference Score (%f) for scope %s and zone %s", score, instanceRef.FlexibilityScopeKey, zone)
		}
		totalScore += score
		count++
		return nil
	}

	for _, ng := range option.SimilarNodeGroups {
		if err := processNodeGroup(ng); err != nil {
			return 0, err
		}
	}
	if err := processNodeGroup(option.NodeGroup); err != nil {
		return 0, err
	}

	if count == 0 {
		return 0, fmt.Errorf("no node groups scored")
	}

	return totalScore / float64(count), nil
}

func getMatchedRule(ccc crd.CRD, opt expander.Option) rules.Rule {
	for _, ruleGroup := range ccc.GroupedRules() {
		for _, rule := range ruleGroup {
			if rule.Matches(opt.NodeGroup) {
				return rule
			}
		}
	}
	return nil
}

func (f *fleetEfficiencyFilter) isFleetEfficiencyStrategy(ccc crd.CRD, opt expander.Option) bool {
	// Check if the matched rule overrides the allocation strategy.
	if strategyRule, ok := getMatchedRule(ccc, opt).(rules.AllocationStrategyRule); ok {
		strategy := strategyRule.AllocationStrategy()
		if strategy != nil && *strategy != "" {
			return *strategy == cccv1.AllocationStrategyFleetEfficiency
		}
	}
	return f.getClusterDefaultAllocationStrategy() == options.ClusterDefaultAllocationStrategyFleetEfficiency
}

func (f *fleetEfficiencyFilter) getClusterDefaultAllocationStrategy() options.ClusterDefaultAllocationStrategy {
	clusterStrategy := f.clusterDefaultAllocationStrategy
	if clusterStrategy == "" {
		expValue := f.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.ClusterDefaultAllocationStrategyFlag, "")
		clusterStrategy = options.ClusterDefaultAllocationStrategy(expValue)
	}
	return clusterStrategy
}

func (f *fleetEfficiencyFilter) hasUsableReservations(expansionOptions []expander.Option) bool {
	if f.reservationsPuller == nil {
		return false
	}
	gceReservations := f.reservationsPuller.GetReservations()
	if len(gceReservations) == 0 {
		return false
	}
	for _, option := range expansionOptions {
		nodeGroups := append(option.SimilarNodeGroups, option.NodeGroup)
		for _, nodeGroup := range nodeGroups {
			if reservations.MatchingUnusedReservations(f.cloudProvider, nodeGroup, gceReservations, f.localSSDDiskSizeProvider) > 0 {
				return true
			}
		}
	}
	return false
}
