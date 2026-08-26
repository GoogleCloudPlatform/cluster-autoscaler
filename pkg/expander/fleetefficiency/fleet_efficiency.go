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
	"errors"
	"fmt"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

var (
	ErrSnapshotNotFound          = errors.New("flex advisor snapshot not found")
	ErrPreferenceScoreNotPresent = errors.New("GCE Preference Score not present")
)

type fleetEfficiencyFilter struct {
	flexAdvisor                      instanceavailability.Provider
	cccLister                        lister.Lister
	fallback                         expander.Strategy
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
	fallback expander.Strategy,
	cloudProvider provider.GkeExpanderCloudProvider,
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider,
	clusterDefaultAllocationStrategy options.ClusterDefaultAllocationStrategy,
	gceFlexAdvisorEnabled bool,
	experimentsManager experiments.Manager,
) *fleetEfficiencyFilter {
	return &fleetEfficiencyFilter{
		flexAdvisor:                      flexAdvisor,
		cccLister:                        cccLister,
		fallback:                         fallback,
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
		// We don't know the allocation strategy, do not record any metrics at this point.
		return expansionOptions
	}
	if crd == nil {
		// Allocation strategy is only supported for CCC, do not record metrics.
		return expansionOptions
	}
	if !f.isFleetEfficiencyStrategySelected(crd, expansionOptions) {
		klog.V(4).Infof("FleetEfficiencyFilter: allocation strategy is not fleet-efficiency (CCC %s), skipping", crd.Name())
		return f.fallbackAndRecordMetric(expansionOptions, nodeInfo, cccv1.AllocationStrategyLowestCost, metrics.AllocationStrategyFallbackNone)
	}

	if f.hasUsableReservations(expansionOptions) {
		klog.V(4).Infof("FleetEfficiencyFilter: some options have usable reservations (CCC %s), skipping", crd.Name())
		return f.fallbackAndRecordMetric(expansionOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackReservationPresent)
	}

	klog.V(4).Infof("FleetEfficiencyFilter: evaluating %d expansion options (CCC %s)", len(expansionOptions), crd.Name())

	// Calculate the scores to find the best options.
	scores := make([]float64, len(expansionOptions))
	maxScore := -1.0
	for i, option := range expansionOptions {
		score, err := f.scoreOption(option, crd)
		if err != nil {
			klog.V(4).Infof("FleetEfficiencyFilter: failed to score option %s (CCC %s), ignoring strategy: %v", option.NodeGroup.Id(), crd.Name(), err)
			return f.fallbackAndRecordMetric(expansionOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, determineFallbackReason(err))
		}

		klog.V(5).Infof("FleetEfficiencyFilter: fleet efficiency score for option %s (CCC %s) is %f", option.NodeGroup.Id(), crd.Name(), score)

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

	if klog.V(5).Enabled() {
		bestOptionIds := make([]string, len(bestOptions))
		for i := range bestOptions {
			bestOptionIds[i] = bestOptions[i].NodeGroup.Id()
		}
		klog.V(5).Infof("FleetEfficiencyFilter: best options (CCC %s, best score %f): %v", crd.Name(), maxScore, bestOptionIds)
	}

	if len(bestOptions) == 1 {
		klog.V(4).Infof("FleetEfficiencyFilter: selected best option %s (CCC %s)", bestOptions[0].NodeGroup.Id(), crd.Name())
		f.recordMetric(cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackNone, &bestOptions[0])
		return bestOptions
	}

	klog.V(4).Infof("FleetEfficiencyFilter: tie break between %d options (CCC %s), fallback to lowest-cost", len(bestOptions), crd.Name())
	return f.fallbackAndRecordMetric(bestOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackTieBreak)
}

func determineFallbackReason(err error) metrics.AllocationStrategyFallbackReason {
	if err == nil {
		return metrics.AllocationStrategyFallbackNone
	}
	if errors.Is(err, flexadvisor.ErrNotSupported) {
		return metrics.AllocationStrategyFallbackFlexAdvisorNotSupported
	} else if errors.Is(err, ErrSnapshotNotFound) || errors.Is(err, ErrPreferenceScoreNotPresent) {
		return metrics.AllocationStrategyFallbackMissingScore
	}
	return metrics.AllocationStrategyFallbackError
}

func (f *fleetEfficiencyFilter) recordMetric(requestedStrategy cccv1.AllocationStrategy, fallbackReason metrics.AllocationStrategyFallbackReason, option *expander.Option) {
	if option == nil {
		klog.Fatal("FleetEfficiencyFilter: recordMetric called with nil option")
		return
	}
	machineType := ""
	if gkeNodeGroup, ok := option.NodeGroup.(gke.NodeGroup); ok {
		machineType = gkeNodeGroup.MachineType()
	}
	metrics.RegisterNodesWithAllocationStrategy(string(requestedStrategy), fallbackReason, machineType, option.NodeCount)
}

func (f *fleetEfficiencyFilter) fallbackAndRecordMetric(expansionOptions []expander.Option, nodeInfo map[string]*framework.NodeInfo, requestedStrategy cccv1.AllocationStrategy, fallbackReason metrics.AllocationStrategyFallbackReason) []expander.Option {
	if f.fallback == nil {
		return expansionOptions
	}
	selected := f.fallback.BestOption(expansionOptions, nodeInfo)
	if selected != nil {
		f.recordMetric(requestedStrategy, fallbackReason, selected)
		return []expander.Option{*selected}
	}
	// This should never happen, since fallback should be gke_price which always returns one option.
	// If it does happen, it's safest to return all options, which will most likely result in fallback being called again (as the next expander).
	klog.Error("FleetEfficiencyFilter: fallback failed to choose the best option.")
	return expansionOptions
}

func (f *fleetEfficiencyFilter) scoreOption(option expander.Option, crd crd.CRD) (float64, error) {
	instanceRef, err := flexadvisor.ConstructInstanceReference(option.NodeGroup, f.cccLister, f.experimentsManager)
	if err != nil {
		return 0, fmt.Errorf("failed to construct instance reference: %w", err)
	}
	snapshot := f.flexAdvisor.GetInstanceAvailability(instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
	if snapshot == nil {
		return 0, fmt.Errorf("%w for keys: scope=%q, config=%s", ErrSnapshotNotFound, instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
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
			return fmt.Errorf("%w for scope %s and zone %s", ErrPreferenceScoreNotPresent, instanceRef.FlexibilityScopeKey, zone)
		}
		if score < 0 || score > 1 {
			// TODO(b/527312993): Move the filtering to flex advisor (reject invalid scores).
			return fmt.Errorf("invalid GCE Preference Score (%f) for scope %s and zone %s", score, instanceRef.FlexibilityScopeKey, zone)
		}
		klog.V(5).Infof("FleetEfficiencyFilter: gce preference score for node group %s is %f (CCC %s)", ng.Id(), score, crd.Name())
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

// isFleetEfficiencyStrategySelected determines whether the fleet-efficiency allocation strategy
// should be used for the given candidate expansion options.
//
// CCC rules sharing the same priorityScore can define conflicting allocation strategies
// (or omit them). To resolve conflicts deterministically across all candidate expansion
// options in the priorityScore group, we enforce the following precedence:
//  1. Explicit non-fleet-efficiency strategy (e.g., lowest-cost): overrides explicit
//     fleet-efficiency and cluster defaults. CCC validation assumes lowest-cost when
//     conflicting strategies are present in the same priority score, so any explicit
//     non-fleet-efficiency strategy takes highest precedence. Note that having conflicting
//     strategies in the same priority score is possible only for a non-default cluster
//     allocation strategy (other than lowest-cost), where one rule explicitly specifies
//     lowest-cost and another omits the strategy (inheriting the cluster default).
//  2. Explicit fleet-efficiency strategy: overrides cluster defaults.
//  3. Omitted (nil) strategy: inherits the cluster default allocation strategy.
func (f *fleetEfficiencyFilter) isFleetEfficiencyStrategySelected(ccc crd.CRD, opts []expander.Option) bool {
	hasFleetEfficiency := false
	for _, opt := range opts {
		if sr, ok := getMatchedRule(ccc, opt).(rules.AllocationStrategyRule); ok {
			strategy := sr.AllocationStrategy()
			if strategy != nil && *strategy != cccv1.AllocationStrategyFleetEfficiency {
				// Explicit non-fleet-efficiency strategy (e.g., lowest-cost) overrides everything in this priorityScore group.
				// Note that conflicting strategies in the same priority score are possible only for a non-default cluster
				// allocation strategy (other than lowest-cost), where one rule specifies lowest-cost and another omits it.
				return false
			}
			if strategy != nil && *strategy == cccv1.AllocationStrategyFleetEfficiency {
				hasFleetEfficiency = true
			}
		}
	}
	if hasFleetEfficiency {
		// Explicit fleet-efficiency overrides cluster default.
		return true
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
