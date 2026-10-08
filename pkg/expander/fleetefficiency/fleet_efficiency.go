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
	"errors"
	"fmt"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/billing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/provider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
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
	// backoff is the scale-up backoff shared between NAP and the core scale-up logic. It is used to
	// exclude zones in which uncreated NAP candidates cannot currently be scaled up. May be nil.
	backoff        base_backoff.Backoff
	optionsTracker *optstracking.OptionsTracker
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
	backoff base_backoff.Backoff,
	optionsTracker *optstracking.OptionsTracker,
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
		backoff:                          backoff,
		optionsTracker:                   optionsTracker,
	}
}

func (f *fleetEfficiencyFilter) BestOptions(ctx context.Context, expansionOptions []expander.Option, nodeInfo map[string]*framework.NodeInfo) []expander.Option {
	if !IsFleetEfficiencyEnabled(f.gceFlexAdvisorEnabled, f.experimentsManager) {
		klog.V(4).Infof("FleetEfficiencyFilter: expander disabled by experiments")
		return expansionOptions
	}

	if len(expansionOptions) == 0 || len(expansionOptions[0].Pods) == 0 {
		klog.V(4).Infof("FleetEfficiencyFilter: no expansion options or no pods in the first option, skipping")
		return expansionOptions
	}

	// Verify the allocation strategy.
	samplePod := expansionOptions[0].Pods[0]
	crd, source, err := f.getPodCrd(samplePod)
	if err != nil {
		klog.Errorf("FleetEfficiencyFilter: failed to get the CRD for pod: %v", err)
		// We don't know the allocation strategy, do not record any metrics at this point.
		return expansionOptions
	}
	if crd == nil {
		// Allocation strategy is only supported for CCC or pay-per-pod autopilot workloads, do not record metrics.
		klog.V(4).Infof("FleetEfficiencyFilter: pod %s/%s does not use a ComputeClass, skipping", samplePod.Namespace, samplePod.Name)
		return expansionOptions
	}
	isFleetEfficiency, effectiveSource := f.isFleetEfficiencyStrategySelected(crd, source, expansionOptions)
	if !isFleetEfficiency {
		klog.V(4).Infof("FleetEfficiencyFilter: allocation strategy is not fleet-efficiency (CCC %s, cluster default %q), skipping", crd.Name(), f.getClusterDefaultAllocationStrategy())
		return f.fallbackAndRecordMetric(ctx, expansionOptions, nodeInfo, cccv1.AllocationStrategyLowestCost, metrics.AllocationStrategyFallbackNone, effectiveSource)
	}

	// The node group sets of every option are resolved once and shared by the reservation check and scoring.
	nodeGroupSets := make([]flexadvisor.NodeGroupSet, len(expansionOptions))
	for i, option := range expansionOptions {
		nodeGroupSets[i] = flexadvisor.NewNodeGroupSet(ctx, option.NodeGroup, option.SimilarNodeGroups, f.cloudProvider)
	}

	if nodeGroupId, found := f.usableReservationNodeGroup(nodeGroupSets); found {
		klog.V(4).Infof("FleetEfficiencyFilter: node group %s has matching unused reservations (CCC %s), falling back to lowest-cost", nodeGroupId, crd.Name())
		return f.fallbackAndRecordMetric(ctx, expansionOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackReservationPresent, effectiveSource)
	}

	if reason, explanation, shouldFallback := shouldFallbackToLowestCost(expansionOptions); shouldFallback {
		klog.V(4).Infof("FleetEfficiencyFilter: %s (CCC %s), falling back to lowest-cost with reason %q", explanation, crd.Name(), reason)
		return f.fallbackAndRecordMetric(ctx, expansionOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, reason, effectiveSource)
	}

	klog.V(4).Infof("FleetEfficiencyFilter: evaluating %d expansion options (CCC %s)", len(expansionOptions), crd.Name())

	// Calculate the scores to find the best options.
	scores := make([]float64, len(expansionOptions))
	maxScore := -1.0
	for i, option := range expansionOptions {
		score, err := f.scoreOption(nodeGroupSets[i], crd, nodeInfo)
		if err != nil {
			nodeGroupId := "unknown"
			if option.NodeGroup != nil {
				nodeGroupId = option.NodeGroup.Id()
			}
			reason := determineFallbackReason(err)
			klog.V(4).Infof("FleetEfficiencyFilter: failed to score option %s (CCC %s), falling back to lowest-cost with reason %q: %v", nodeGroupId, crd.Name(), reason, err)
			return f.fallbackAndRecordMetric(ctx, expansionOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, reason, effectiveSource)
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
		f.recordMetric(cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackNone, effectiveSource, &bestOptions[0])
		return bestOptions
	}

	klog.V(4).Infof("FleetEfficiencyFilter: tie break between %d options (CCC %s), fallback to lowest-cost", len(bestOptions), crd.Name())
	return f.fallbackAndRecordMetric(ctx, bestOptions, nodeInfo, cccv1.AllocationStrategyFleetEfficiency, metrics.AllocationStrategyFallbackTieBreak, effectiveSource)
}

// shouldFallbackToLowestCost returns whether fleet efficiency cannot be used for the given options because
// of unsupported constraints, together with the metric reason and a human-readable explanation.
func shouldFallbackToLowestCost(options []expander.Option) (metrics.AllocationStrategyFallbackReason, string, bool) {
	// 1. Fall back if pods have any zonal, topology, or stateful constraints.
	for _, opt := range options {
		for _, pod := range opt.Pods {
			if constraint, found := zonalConstraint(pod); found {
				return metrics.AllocationStrategyFallbackUnsupported, fmt.Sprintf("pod %s/%s has %s", pod.Namespace, pod.Name, constraint), true
			}
		}
	}

	// 2. Fall back if any option has compact placement, TPU multi-host, or targets a specific reservation.
	for _, opt := range options {
		gkeNg, ok := opt.NodeGroup.(gke.NodeGroup)
		if !ok {
			continue
		}
		spec := gkeNg.Spec()
		if spec == nil {
			continue
		}
		if spec.PlacementGroup.UsesPlacement() {
			return metrics.AllocationStrategyFallbackUnsupported, fmt.Sprintf("node group %s uses placement policy", gkeNg.Id()), true
		}
		if spec.TpuMultiHost {
			return metrics.AllocationStrategyFallbackUnsupported, fmt.Sprintf("node group %s is TPU multi-host", gkeNg.Id()), true
		}
		// Only SPECIFIC_RESERVATION pins the node pool to a particular reservation (and zone).
		// ANY_RESERVATION (the GKE default for Standard node pools) and unspecified affinities do not constrain
		// placement; whether matching unused reservations actually exist is checked by usableReservationNodeGroup.
		if spec.ReservationAffinity != nil && spec.ReservationAffinity.ConsumeReservationType == gkeclient.ReservationAffinitySpecific {
			return metrics.AllocationStrategyFallbackReservationPresent, fmt.Sprintf("node group %s targets a specific reservation", gkeNg.Id()), true
		}
	}

	return metrics.AllocationStrategyFallbackNone, "", false
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

func (f *fleetEfficiencyFilter) recordMetric(requestedStrategy cccv1.AllocationStrategy, fallbackReason metrics.AllocationStrategyFallbackReason, allocationStrategySource metrics.AllocationStrategySource, option *expander.Option) {
	if option == nil {
		klog.Fatal("FleetEfficiencyFilter: recordMetric called with nil option")
		return
	}
	machineType := ""
	if gkeNodeGroup, ok := option.NodeGroup.(gke.NodeGroup); ok {
		machineType = gkeNodeGroup.MachineType()
	}
	metrics.RegisterNodesWithAllocationStrategy(string(requestedStrategy), fallbackReason, machineType, option.NodeCount)
	metrics.RegisterScaleupsWithAllocationStrategy(string(requestedStrategy), fallbackReason, allocationStrategySource)
}

func (f *fleetEfficiencyFilter) fallbackAndRecordMetric(ctx context.Context, expansionOptions []expander.Option, nodeInfo map[string]*framework.NodeInfo, requestedStrategy cccv1.AllocationStrategy, fallbackReason metrics.AllocationStrategyFallbackReason, allocationStrategySource metrics.AllocationStrategySource) []expander.Option {
	if f.fallback == nil {
		return expansionOptions
	}
	selected := f.fallback.BestOption(ctx, expansionOptions, nodeInfo)
	if selected != nil {
		f.recordMetric(requestedStrategy, fallbackReason, allocationStrategySource, selected)
		return []expander.Option{*selected}
	}
	// This should never happen, since fallback should be gke_price which always returns one option.
	// If it does happen, it's safest to return all options, which will most likely result in fallback being called again (as the next expander).
	klog.Error("FleetEfficiencyFilter: fallback failed to choose the best option.")
	return expansionOptions
}

func (f *fleetEfficiencyFilter) scoreOption(nodeGroupSet flexadvisor.NodeGroupSet, crd crd.CRD, nodeInfos map[string]*framework.NodeInfo) (float64, error) {
	nodeGroup := nodeGroupSet.Representative()
	instanceRef, err := flexadvisor.ConstructInstanceReferenceForScope(nodeGroup, crd.Name(), f.experimentsManager)
	if err != nil {
		return 0, fmt.Errorf("failed to construct instance reference: %w", err)
	}
	snapshot := f.flexAdvisor.GetInstanceAvailability(instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
	if snapshot == nil {
		return 0, fmt.Errorf("%w for keys: scope=%q, config=%s", ErrSnapshotNotFound, instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
	}

	targetZones, err := nodeGroupSet.TargetZones(f.backoff, nodeInfos)
	if err != nil {
		return 0, err
	}
	if len(targetZones) == 0 {
		// Only uncreated candidates can end up without target zones, when all of them are backed off.
		// Score the candidate with 0 so that it loses to any healthy option (> 0). If all candidates
		// are backed off (all score 0), they tie at 0 and tie-breaking falls back to lowest-cost.
		klog.V(5).Infof("FleetEfficiencyFilter: all target zones are backed off for uncreated node group %s, scoring 0", nodeGroup.Id())
		return 0.0, nil
	}

	totalScore := 0.0
	for _, zone := range targetZones {
		score, found := snapshot.GcePreferenceScore(zone)
		if !found {
			return 0, fmt.Errorf("%w for scope %s and zone %s", ErrPreferenceScoreNotPresent, instanceRef.FlexibilityScopeKey, zone)
		}
		if score < 0 || score > 1 {
			// TODO(b/527312993): Move the filtering to flex advisor (reject invalid scores).
			return 0, fmt.Errorf("invalid GCE Preference Score (%f) for scope %s and zone %s", score, instanceRef.FlexibilityScopeKey, zone)
		}
		klog.V(5).Infof("FleetEfficiencyFilter: gce preference score for node group %s in zone %s is %f (CCC %s)", nodeGroup.Id(), zone, score, crd.Name())
		totalScore += score
	}

	return totalScore / float64(len(targetZones)), nil
}

func (f *fleetEfficiencyFilter) getPodCrd(pod *apiv1.Pod) (crd.CRD, metrics.AllocationStrategySource, error) {
	if pod == nil {
		return nil, metrics.AllocationStrategySourceUnknown, nil
	}
	crd, name, err := f.cccLister.PodCrd(pod)
	if err != nil {
		return nil, metrics.AllocationStrategySourceUnknown, err
	}
	if crd != nil {
		return crd, metrics.AllocationStrategySourceCustomComputeClass, nil
	}
	if f.cloudProvider != nil && f.cloudProvider.IsAutopilotEnabled() && experiments.IsPayPerPodFleetEfficiencyEnabled(f.experimentsManager) {
		projectId, _, _ := f.cloudProvider.GetClusterInfo()
		if machinetypes.IsPredefinedComputeClass(name) {
			if !flexadvisor.IsFlexAdvisorPCCSupportEnabled(f.experimentsManager) {
				return nil, metrics.AllocationStrategySourceUnknown, nil
			}
			if billing.GetBillingModel(pod, nil, name, true) == billing.PodBasedBilling {
				pcc, err := machinetypes.ToPredefinedComputeClass(name)
				if err != nil {
					return nil, metrics.AllocationStrategySourceUnknown, err
				}
				return ccc.NewPccCrd(pcc, projectId, true, f.cloudProvider, f.optionsTracker), metrics.AllocationStrategySourcePredefinedComputeClass, nil
			}
			return nil, metrics.AllocationStrategySourceUnknown, nil
		}
		if podFamily, ok := billing.GetPodFamilyForPayPerPodAutopilotWorkload(pod); ok {
			return ccc.NewPodFamilyCrd(podFamily, projectId, true, f.cloudProvider, f.optionsTracker), metrics.AllocationStrategySourceAutopilotWorkload, nil
		}
	}
	return nil, metrics.AllocationStrategySourceUnknown, nil
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
// should be used for the given candidate expansion options, along with the resolved strategy source.
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
func (f *fleetEfficiencyFilter) isFleetEfficiencyStrategySelected(ccc crd.CRD, crdSource metrics.AllocationStrategySource, opts []expander.Option) (bool, metrics.AllocationStrategySource) {
	hasFleetEfficiency := false
	for _, opt := range opts {
		if sr, ok := getMatchedRule(ccc, opt).(rules.AllocationStrategyRule); ok {
			strategy := sr.AllocationStrategy()
			if strategy != nil && *strategy != cccv1.AllocationStrategyFleetEfficiency {
				// Explicit non-fleet-efficiency strategy (e.g., lowest-cost) overrides everything in this priorityScore group.
				// Note that conflicting strategies in the same priority score are possible only for a non-default cluster
				// allocation strategy (other than lowest-cost), where one rule specifies lowest-cost and another omits it.
				return false, crdSource
			}
			if strategy != nil && *strategy == cccv1.AllocationStrategyFleetEfficiency {
				hasFleetEfficiency = true
			}
		}
	}
	if hasFleetEfficiency {
		// Explicit fleet-efficiency overrides cluster default.
		return true, crdSource
	}
	return f.getClusterDefaultAllocationStrategy() == options.ClusterDefaultAllocationStrategyFleetEfficiency, metrics.AllocationStrategySourceCluster
}

// getClusterDefaultAllocationStrategy returns the cluster default allocation strategy, taken from the CLI flag
// or, if unset, from experiments. Returns an empty strategy (i.e. lowest-cost) if the override is disabled by
// experiments.
func (f *fleetEfficiencyFilter) getClusterDefaultAllocationStrategy() options.ClusterDefaultAllocationStrategy {
	if !IsDefaultAllocationStrategyEnabled(f.experimentsManager) {
		return ""
	}
	clusterStrategy := f.clusterDefaultAllocationStrategy
	if clusterStrategy == "" {
		expValue := f.experimentsManager.EvaluateStringFlagOrFailsafe(experiments.ClusterDefaultAllocationStrategyFlag, "")
		clusterStrategy = options.ClusterDefaultAllocationStrategy(expValue)
	}
	return clusterStrategy
}

// usableReservationNodeGroup returns the id of the first zonal node group in the given options that
// has matching unused reservations, and whether such a node group was found. For uncreated candidates,
// all planned zones are checked.
func (f *fleetEfficiencyFilter) usableReservationNodeGroup(nodeGroupSets []flexadvisor.NodeGroupSet) (string, bool) {
	if f.reservationsPuller == nil {
		return "", false
	}
	gceReservations := f.reservationsPuller.GetReservations()
	if len(gceReservations) == 0 {
		return "", false
	}
	for _, ngs := range nodeGroupSets {
		for _, nodeGroup := range ngs.NodeGroups() {
			if reservations.MatchingUnusedReservations(f.cloudProvider, nodeGroup, gceReservations, f.localSSDDiskSizeProvider) > 0 {
				return nodeGroup.Id(), true
			}
		}
	}
	return "", false
}
