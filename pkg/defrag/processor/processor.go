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

package processor

import (
	"context"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/observability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	cacontext "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/pdb"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
)

const DefragProcessorName = "defrag"

type removeCandidateReason string

const (
	scaleUpTimeoutExceeded   removeCandidateReason = "scale_up_timeout"
	scaleDownTimeoutExceeded removeCandidateReason = "scale_down_timeout"
	noValidNodes             removeCandidateReason = "no_valid_nodes"
	noScaleUpOptions         removeCandidateReason = "no_scale_up_options"
)

type Config struct {
	CandidateLimit int
	// The MaxDelay is not used anymore as defrag is not gated by the MaxDelay.
	// It is rather bounded by shared enforcer which uses a global delay which spans
	// accross multiple processors.
	MaxDelay         time.Duration
	ScaleUpTimeout   time.Duration
	ScaleDownTimeout time.Duration
	ScaleDownDelay   time.Duration
	Autopilot        bool
	// RecordBlockReasons makes every pass explain why each node outside its
	// candidates cannot be migrated, and publishes the result through
	// BlockReasons. A pass that stops early, because a tracked candidate still
	// needs its scale-up or because the candidate limit is reached, has to run
	// an extra candidate search for that, which costs about as much as the
	// search a pass without tracked candidates performs anyway. Leave it off
	// when nothing reads BlockReasons.
	RecordBlockReasons bool
}

// candidateInfo keeps the metadata about candidates
type candidateInfo struct {
	candidate          *defrag.Candidate
	creationTime       time.Time
	scaleUpNoOptions   bool
	defragPossible     bool
	defragPossibleTime time.Time
	waitingForScaleUp  bool
	defragPossibleMap  map[string]time.Time
}

func (info *candidateInfo) String() string {
	return info.candidate.String()
}

// reorganizeNodes reorganizes candidate by pushing removable nodes to the front,
// so they will be processed first in the next iteration.
func (info *candidateInfo) reorganizeNodes() {
	if info.candidate.IsAtomic {
		return
	}
	var removableNodes, unremovableNodes []string
	for _, node := range info.candidate.Nodes {
		if _, ok := info.defragPossibleMap[node]; ok {
			removableNodes = append(removableNodes, node)
		} else {
			unremovableNodes = append(unremovableNodes, node)
		}
	}
	info.candidate.Nodes = append(removableNodes, unremovableNodes...)
}

// hasPendingScaleDowns checks if any candidate node is waiting for scale down.
func (info *candidateInfo) hasPendingScaleDowns() bool {
	for _, node := range info.candidate.Nodes {
		if _, ok := info.defragPossibleMap[node]; ok {
			return true
		}
	}
	return false
}

// Processor implements the defrag algorithm
type Processor struct {
	actuator          *defragActuator
	backoff           *defragBackoff
	nodeFilterFactory *defragNodeFilterFactory
	simulator         *defragSimulator
	fairnessEnforcer  fairness.FairnessEnforcer
	nodeReconciler    *nodeReconciler
	// pdbTracker is used to track remaining PDBs in the scope of defrag, ensuring
	// that current and new candidates can be removed without exceeding PDBs.
	// The global RemainingPdbTracker is updated only when nodes are scaled down,
	// to ensure that scale-down won't accidentally exceed PDBs in this CA loop.
	pdbTracker pdb.RemainingPdbTracker

	config  Config
	plugins []defrag.Plugin

	candidateInfos      []*candidateInfo
	pickedCandidateInfo *candidateInfo

	ctx   *cacontext.AutoscalingContext
	clock clock.PassiveClock

	// blockReasons holds why each node could not be migrated during the defrag
	// pass of the current loop. It is nil when no pass completed, and reading
	// it clears it, so it can never outlive the loop that produced it.
	blockReasons *observability.Registry

	experimentsManager experiments.Manager
}

type Options struct {
	ScaleDownNodeProcessor   nodes.ScaleDownNodeProcessor
	ScaleDownStatusProcessor status.ScaleDownStatusProcessor
	DeleteOptions            options.NodeDeleteOptions
	DrainabilityRules        rules.Rules
	Config                   Config
	Plugins                  []defrag.Plugin
	Clock                    clock.PassiveClock
	ExperimentsManager       experiments.Manager
	FairnessEnforcer         fairness.FairnessEnforcer
	MinQuotasTrackerFactory  *resourcequotas.TrackerFactory
	CcLister                 lister.Lister
}

// NewProcessor returns the default implementation of defrag processor
func NewProcessor(opts Options) *Processor {
	c := opts.Clock
	if c == nil {
		c = clock.RealClock{}
	}
	return &Processor{
		actuator: newDefragActuator(actuatorOptions{
			ScaleDownStatusProcessor: opts.ScaleDownStatusProcessor,
			Clock:                    c,
		}),
		backoff:            newDefragBackoff(),
		nodeFilterFactory:  newDefragNodeFilterFactory(opts.ScaleDownNodeProcessor, opts.DeleteOptions, opts.DrainabilityRules, opts.MinQuotasTrackerFactory, opts.CcLister),
		simulator:          newSimulator(simulatorOptions{DeleteOptions: opts.DeleteOptions, DrainabilityRules: opts.DrainabilityRules}),
		fairnessEnforcer:   opts.FairnessEnforcer,
		nodeReconciler:     newNodeReconciler(nodeReconcilerOptions{}),
		pdbTracker:         pdb.NewBasicRemainingPdbTracker(),
		config:             opts.Config,
		plugins:            opts.Plugins,
		clock:              c,
		experimentsManager: opts.ExperimentsManager,
	}
}

// Process returns pods that defrag would like to consider for scale-up
func (p *Processor) Process(ctx context.Context, autoscalingCtx *cacontext.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	p.ctx = autoscalingCtx
	p.pickedCandidateInfo = nil
	p.blockReasons = nil

	nodeFilter, err := p.nodeFilterFactory.NewDefragNodeFilter(p.ctx)
	if err != nil {
		return nil, err
	}

	if p.fairnessEnforcer != nil && !p.fairnessEnforcer.Admit(unschedulablePods) {
		// Defrag did not evaluate any node this loop, so there is nothing to
		// say about which nodes are blocked.
		return unschedulablePods, nil
	}

	if err := p.cleanUpCandidates(nodeFilter); err != nil {
		return nil, err
	}
	if err := p.nodeReconciler.Reconcile(autoscalingCtx, p.candidateInfos); err != nil {
		return nil, err
	}
	pods, err := p.processCandidates(nodeFilter)
	if err != nil {
		return nil, err
	}

	// Only a pass that ran to completion is a complete answer about what is
	// blocked. A pass that gave up part way leaves the registry holding
	// whatever the filters reached before the error, and publishing that would
	// report every node they never got to as free to migrate. The same goes
	// for a pass that was not asked to record: its filters only saw the nodes
	// the search happened to reach.
	if p.config.RecordBlockReasons {
		p.blockReasons = nodeFilter.BlockReasons()
	}

	klog.V(4).Infof("Defrag candidate count: %v", len(p.candidateInfos))

	if p.pickedCandidateInfo != nil {
		klog.V(4).Infof("Defrag candidate %v picked", p.pickedCandidateInfo)
		// Tag the pods we surface to scale-up, so that status reporting can
		// attribute the resulting node provisioning to active migration.
		return markActiveMigrationPods(pods), nil
	}

	klog.V(4).Infof("No defrag candidates, restoring unschedulable pods")
	return unschedulablePods, nil
}

// markActiveMigrationPods returns copies of pods annotated as active migration
// pods. The input pods and the annotation maps they share with the cluster
// snapshot are left untouched.
func markActiveMigrationPods(pods []*apiv1.Pod) []*apiv1.Pod {
	marked := make([]*apiv1.Pod, 0, len(pods))
	for _, pod := range pods {
		markedPod := pod.DeepCopy()
		if markedPod.Annotations == nil {
			markedPod.Annotations = make(map[string]string, 1)
		}
		markedPod.Annotations[defrag.ActiveMigrationPodAnnotation] = "true"
		marked = append(marked, markedPod)
	}
	return marked
}

// BlockReasons returns why each node was held back during the defrag pass of
// the current autoscaler loop, or nil if no pass completed or recording is
// disabled by Config.RecordBlockReasons.
//
// A nil result means "unknown", not "nothing is blocked": callers must not
// treat it as an empty set, or a loop in which defrag was skipped would look
// like a loop in which every node was free to migrate.
//
// Reading consumes the result. Only a completed pass publishes one, so a loop
// in which Process did not run has no answer to give, and handing back the
// previous loop's would present stale reasons as current.
func (p *Processor) BlockReasons() *observability.Registry {
	reasons := p.blockReasons
	p.blockReasons = nil
	return reasons
}

// DefragPickedCandidate returns true if defrag picked a candidate during the last Process call.
func (p *Processor) DefragPickedCandidate() bool {
	return p.pickedCandidateInfo != nil
}

// cleanUpCandidates sanitises and validates the currently tracked defrag Candidates
func (p *Processor) cleanUpCandidates(filter *defragNodeFilter) error {
	// Initialize PDB Tracker
	if err := p.pdbTracker.SetPdbs(p.ctx.RemainingPdbTracker.GetPdbs()); err != nil {
		klog.Errorf("Error while setting defrag PDB tracker: %v", err)
		return err
	}

	// Clean up candidates
	var candidates []*defrag.Candidate
	var candidateInfos []*candidateInfo
	for _, info := range p.candidateInfos {
		isScaledDown := p.actuator.isScaleDownFullyStarted(info.candidate)
		filter.filterDeletedCandidateNodes(p.ctx, info.candidate)
		survivingCount := len(info.candidate.Nodes)

		filter.filterInvalidCandidateNodes(p.ctx, p.pdbTracker, info.candidate)

		nodes, err := filter.filterNodesViolatingMinQuotas(p.ctx, info.candidate.Nodes)
		if err != nil {
			klog.Errorf("Defrag: failed to filter nodes violating min quotas for candidate %v: %v", info, err)
			return err
		}
		info.candidate.Nodes = filter.filterNodesViolatingMinSize(p.ctx, nodes)

		if info.candidate.IsAtomic && len(info.candidate.Nodes) < survivingCount {
			klog.V(1).Infof("Atomic candidate %v lost nodes, invalidating candidate", info)
			// The nodes that were filtered out already carry the reason that
			// removed them. The ones left behind are blocked only because the
			// group can no longer be migrated as a whole.
			for _, nodeName := range info.candidate.Nodes {
				filter.BlockReasons().Record(nodeName, observability.AtomicGroupBlocked)
			}
			info.candidate.Nodes = nil
		}

		if shouldRemove, reason := p.shouldRemoveCandidate(info); shouldRemove {
			if reason == noValidNodes && isScaledDown {
				klog.V(4).Infof("Defrag candidate %v fully scaled down", info)
			} else {
				metrics.Metrics.IncrementDefragInvalidatedCandidatesTotal(string(reason), info.candidate.Plugin.String())
				klog.V(4).Infof("Defrag candidate %v no longer valid: %v", info, reason)
				blockReason := blockReasonForRemovedCandidate(reason)
				// Recorded here as well as on the backoff entry. The backoff
				// covers later passes, but newCandidate may not be reached in
				// this one, for example once the candidate limit is hit, and
				// these nodes would then go unattributed for the loop.
				for _, nodeName := range info.candidate.Nodes {
					filter.BlockReasons().Record(nodeName, blockReason)
				}
				p.backoff.backoff(p.ctx, info.candidate, blockReason)
			}
			// The leftover nodes
			metrics.Metrics.IncreaseDefragFailedScaleDownNodesTotal(info.candidate.Plugin.String(), len(info.candidate.Nodes))
			continue
		}

		if !isScaledDown {
			filter.reserveMaxDisruptionBudget(p.ctx, info.candidate, p.actuator.isNodeScaleDownStarted)
		}

		pods, err := recreatablePods(p.ctx.ClusterSnapshot, info.candidate.Nodes)
		if err != nil {
			klog.Errorf("Error while getting pods for defrag candidate %v: %v", info, err)
			return err
		}

		p.pdbTracker.RemovePods(pods)
		klog.V(4).Infof("Keep defrag candidate %v", info)
		info.reorganizeNodes()
		candidates = append(candidates, info.candidate)
		candidateInfos = append(candidateInfos, info)
	}
	p.candidateInfos = candidateInfos
	p.actuator.cleanScaleDownInfo(candidates)
	p.backoff.cleanBackoffInfo()
	return nil
}

// shouldRemoveCandidate checks if candidate should be removed
func (p *Processor) shouldRemoveCandidate(info *candidateInfo) (bool, removeCandidateReason) {
	// Clean up candidates with all nodes filtered out
	if len(info.candidate.Nodes) == 0 {
		return true, noValidNodes
	}
	// Clean up candidates exceeding defrag timeout limit
	if !p.actuator.isScaleDownFullyStarted(info.candidate) && p.clock.Since(info.creationTime) > p.config.ScaleUpTimeout {
		return true, scaleUpTimeoutExceeded
	}
	// Clean up candidates which supposed to trigger scale up and had no scale up option.
	// Wait for upcoming nodes, so partial defrag can do its job, even if there are no
	// further scale up options.
	if !info.waitingForScaleUp && !info.hasPendingScaleDowns() && info.scaleUpNoOptions && info.candidate.Plugin.Type() != defrag.ResizesOnlyPluginType {
		return true, noScaleUpOptions
	}
	// Clean up scaled-down candidates over timeout limit
	if p.actuator.isScaleDownTimedOut(info.candidate, p.config.ScaleDownTimeout) {
		return true, scaleDownTimeoutExceeded
	}
	return false, ""
}

// processCandidates iterates over candidates, updates their status and returns
// pods that should be considered for scale-up. The pods are taken from the first
// Candidate which pods cannot schedule on other existing/upcoming nodes
func (p *Processor) processCandidates(filter *defragNodeFilter) ([]*apiv1.Pod, error) {
	allCandidatesNodes := make(map[string]bool)
	searched := false
	var pickedPods []*apiv1.Pod
	for idx := 0; idx < p.config.CandidateLimit && pickedPods == nil; idx++ {
		// Find a new candidate if needed
		if idx >= len(p.candidateInfos) {
			searched = true
			newCandidateInfo, err := p.newCandidate(filter, allCandidatesNodes)
			if err != nil {
				klog.Error("Error while creating a new defrag candidate")
				return nil, err
			}
			if newCandidateInfo == nil {
				return nil, nil
			}
			klog.V(4).Infof("New defrag candidate %v", newCandidateInfo)
			p.candidateInfos = append(p.candidateInfos, newCandidateInfo)
			filter.reserveMaxDisruptionBudget(p.ctx, newCandidateInfo.candidate, p.actuator.isNodeScaleDownStarted)
		}

		for _, nodeName := range p.candidateInfos[idx].candidate.Nodes {
			allCandidatesNodes[nodeName] = true
		}

		unschedulablePods, err := p.processCandidate(p.candidateInfos[idx], allCandidatesNodes)
		if err != nil {
			klog.Errorf("Error while processing candidate %v: %v", p.candidateInfos[idx], err)
			return nil, err
		}
		if len(unschedulablePods) > 0 {
			p.pickedCandidateInfo = p.candidateInfos[idx]
			p.pickedCandidateInfo.scaleUpNoOptions = true
			pickedPods = unschedulablePods
		}
	}

	// The filters that explain why a node is held back only run while
	// searching for a new candidate. A pass that stops before it gets to
	// search, because an existing candidate still needs its scale-up or
	// because the candidate limit is reached, would otherwise publish a
	// registry that says nothing about the nodes outside its candidates, and
	// nothing recorded reads as free to migrate. The extra search is only
	// worth its cost when something reads the result.
	if !searched && p.config.RecordBlockReasons {
		if err := p.recordBlockedNodes(filter); err != nil {
			return nil, err
		}
	}
	return pickedPods, nil
}

// recordBlockedNodes records why each node outside the tracked candidates
// cannot be migrated, by running the candidate search for its side effects on
// the filter alone. The candidate it may find is discarded: nothing is tainted,
// reserved or tracked for it, so it can be found again by a later pass.
func (p *Processor) recordBlockedNodes(filter *defragNodeFilter) error {
	allCandidatesNodes := make(map[string]bool)
	for _, info := range p.candidateInfos {
		for _, nodeName := range info.candidate.Nodes {
			allCandidatesNodes[nodeName] = true
		}
	}
	_, err := p.searchCandidate(filter, allCandidatesNodes)
	return err
}

// processCandidate processes a single defrag Candidate and returns its unschedulable pods if any exist
func (p *Processor) processCandidate(info *candidateInfo, allCandidatesNodes map[string]bool) ([]*apiv1.Pod, error) {
	if !info.candidate.IsAtomic {
		return p.processCandidatePartial(info, allCandidatesNodes)
	} else {
		return p.processCandidateAtomic(info, allCandidatesNodes)
	}
}

// processCandidatePartial scales down nodes that can be already removed,
// then simulates scheduling on the remaining nodes and returns unschedulable pods.
func (p *Processor) processCandidatePartial(info *candidateInfo, allCandidatesNodes map[string]bool) ([]*apiv1.Pod, error) {
	candidateNodeInfos := make(map[string]*framework.NodeInfo)
	for _, nodeName := range info.candidate.Nodes {
		nodeInfo, err := p.ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			return nil, err
		}
		candidateNodeInfos[nodeName] = nodeInfo
	}
	// simulateNodeRemovals intentionally removes removableNodes from the snapshot.
	// We need the results of the simulation to persist, so the main scale down does not
	// consider replacement nodes as removable.
	removableNodes, unremovableNodes, err := p.simulator.simulateNodeRemovals(p.ctx, info.candidate.Nodes, allCandidatesNodes)
	if err != nil {
		return nil, err
	}
	if info.defragPossibleMap == nil {
		info.defragPossibleMap = make(map[string]time.Time)
	}
	var nodesToScaleDown []string
	for _, node := range removableNodes {
		if _, ok := info.defragPossibleMap[node]; !ok {
			info.defragPossibleMap[node] = p.clock.Now()
		}
		defragPossibleTime := info.defragPossibleMap[node]
		if info.candidate.Mode == defrag.DeleteBeforeCreate || p.clock.Since(defragPossibleTime) >= p.config.ScaleDownDelay {
			nodesToScaleDown = append(nodesToScaleDown, node)
		}
	}
	for _, node := range unremovableNodes {
		delete(info.defragPossibleMap, node)
	}
	// simulateNodeRemovals intentionally does not commit simulations for unremovable nodes.
	// We perform a simulation for them here to get unschedulable pods.
	candidatePods, err := p.simulator.simulatePodsScheduling(p.ctx.ClusterSnapshot, unremovableNodes, allCandidatesNodes)
	if err != nil {
		return nil, err
	}
	info.waitingForScaleUp = len(candidatePods.schedulableOnUpcoming) > 0
	if len(nodesToScaleDown) > 0 {
		// Actuation needs to access nodeInfos of removed nodes to update PDB tracker,
		// send metrics and report scale down status. Therefore, we need to add
		// nodes removed by removal simulator and revert the snapshot after
		// starting the scale down.
		p.ctx.ClusterSnapshot.Fork()
		for _, nodeName := range nodesToScaleDown {
			nodeInfo := candidateNodeInfos[nodeName]
			if err := p.ctx.ClusterSnapshot.AddNodeInfo(nodeInfo); err != nil {
				p.ctx.ClusterSnapshot.Revert()
				return nil, err
			}
		}
		scaledDownNodes, err := p.actuator.startScaleDownNodes(p.ctx, info.candidate, info.creationTime, nodesToScaleDown)
		p.ctx.ClusterSnapshot.Revert()
		if err != nil {
			return nil, err
		}
		metrics.Metrics.IncreaseDefragScaleDownNodesTotal(info.candidate.Plugin.String(), len(scaledDownNodes))
	}
	klog.V(4).Infof(
		"Defrag candidate %v scheduling simulation (partial) - %d nodes scaled down, %d nodes left. Pods: %d on existing, %d on upcoming, %d unschedulable",
		info.candidate,
		len(nodesToScaleDown),
		len(unremovableNodes),
		len(candidatePods.schedulableOnExisting),
		len(candidatePods.schedulableOnUpcoming),
		len(candidatePods.unschedulable),
	)
	return candidatePods.unschedulable, nil
}

// processCandidateAtomic scales down an entire candidate at once and only if
// all pods from candidate nodes can be rescheduled on other nodes in the cluster
// or defrag mode is DeleteBeforeCreate. Returns unschedulable pods.
func (p *Processor) processCandidateAtomic(info *candidateInfo, allCandidatesNodes map[string]bool) ([]*apiv1.Pod, error) {
	// Simulate candidate pods scheduling
	candidatePods, err := p.simulator.simulatePodsScheduling(p.ctx.ClusterSnapshot, info.candidate.Nodes, allCandidatesNodes)
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof(
		"Defrag candidate %v scheduling simulation - %d on existing, %d on upcoming, %d unschedulable",
		info.candidate,
		len(candidatePods.schedulableOnExisting),
		len(candidatePods.schedulableOnUpcoming),
		len(candidatePods.unschedulable),
	)

	if p.actuator.isScaleDownFullyStarted(info.candidate) {
		return candidatePods.unschedulable, nil
	}

	if len(candidatePods.schedulableOnUpcoming) > 0 || len(candidatePods.unschedulable) > 0 {
		info.defragPossible = false
	} else if !info.defragPossible {
		info.defragPossible = true
		info.defragPossibleTime = p.clock.Now()
	}

	if info.defragPossible {
		klog.V(4).Infof("Defrag possible for candidate %v for last %v", info, p.clock.Since(info.defragPossibleTime))
	}

	// Start scale-down if all are schedulable on ready nodes OR candidate mode is "delete before create"
	if info.candidate.Mode == defrag.DeleteBeforeCreate || (info.defragPossible && p.clock.Since(info.defragPossibleTime) >= p.config.ScaleDownDelay) {
		klog.V(4).Infof("Scaling down defrag candidate %v", info)
		scaledDownNodes, err := p.actuator.startScaleDown(p.ctx, info.candidate, info.creationTime)
		if err != nil {
			return nil, err
		}
		metrics.Metrics.IncreaseDefragScaleDownNodesTotal(info.candidate.Plugin.String(), len(scaledDownNodes))
	}

	return candidatePods.unschedulable, nil
}

// newCandidate returns a new defrag Candidate using the Plugins
func (p *Processor) newCandidate(filter *defragNodeFilter, allCandidateNodes map[string]bool) (*candidateInfo, error) {
	candidate, err := p.searchCandidate(filter, allCandidateNodes)
	if err != nil || candidate == nil {
		return nil, err
	}

	klog.V(4).Infof("Creating new defrag candidate for plugin: %s, nodes: %s", candidate.Plugin.String(), strings.Join(candidate.Nodes, ","))
	pods, err := recreatablePods(p.ctx.ClusterSnapshot, candidate.Nodes)
	if err != nil {
		return nil, err
	}

	p.pdbTracker.RemovePods(pods)
	ci := &candidateInfo{candidate: candidate, creationTime: p.clock.Now()}
	if err := p.nodeReconciler.ReconcileCandidate(p.ctx, ci); err != nil {
		return nil, err
	}
	return ci, nil
}

// searchCandidate asks the plugins for a new candidate among the nodes that
// are not part of a tracked candidate, and returns it narrowed down to the
// nodes that may be migrated now, or nil if there is none.
//
// Every node it turns down along the way is recorded in the filter with the
// reason. The processor itself keeps nothing from the search, so its result
// may be discarded; callers that run it for the recording alone should know
// that the plugins do: each updates its unfit-node count and the metric
// derived from it, and a plugin that gives nodes a grace period before it
// offers them (the daemonset one) starts and advances those timers whenever
// it is asked, so a node that turns unfit while a migration is in flight is
// offered as soon as its grace period ends rather than after the migration.
func (p *Processor) searchCandidate(filter *defragNodeFilter, allCandidateNodes map[string]bool) (*defrag.Candidate, error) {
	nodeNames, err := filter.newValidCandidateNodes(p.ctx, p.pdbTracker, allCandidateNodes)
	if err != nil {
		return nil, err
	}

	for _, plugin := range p.plugins {
		availableNodes, backedOffNodes := p.backoff.splitNodesBasedOnBackoff(plugin, nodeNames)
		// A node backed off for this plugin may still be picked up by a later
		// one. That is fine: a node that ends up in a candidate is reported as
		// migrating, which takes precedence over any reason recorded here.
		//
		// The reason comes from the backoff entry rather than being assumed,
		// so a candidate abandoned for lack of replacement capacity keeps
		// reporting that for as long as it stays backed off.
		//
		// The backoff is per plugin, the recording is not: a node that no
		// plugin picks this pass carries the cause of whichever plugin backed
		// it off, even one unrelated to why it is reported on. That is an
		// accepted imprecision. The cause is still a real, recent failure to
		// migrate the node, and it is more useful than the catch-all it would
		// otherwise be reported under.
		for _, nodeName := range backedOffNodes {
			filter.BlockReasons().Record(nodeName, p.backoff.cause(plugin, nodeName))
		}
		candidate := plugin.NewCandidate(p.ctx, availableNodes)
		if reporter, ok := plugin.(defrag.AtomicGroupReporter); ok {
			recordAtomicGroupBlocked(filter, reporter.LatestAtomicGroupBlockedNodes())
		}
		unfitNodesCount := plugin.LatestUnfitNodesCount()
		metrics.Metrics.SetDefragUnfitNodes(plugin.String(), unfitNodesCount+len(backedOffNodes))
		metrics.Metrics.ObserveDefragStaleness(plugin.String())
		if candidate != nil {
			originalNodeCount := len(candidate.Nodes)
			nodes, err := filter.filterNodesViolatingMinQuotas(p.ctx, candidate.Nodes)
			if err != nil {
				klog.Errorf("Defrag: failed to filter nodes violating min quotas for plugin %s: %v", plugin.String(), err)
				continue
			}
			if candidate.IsAtomic && len(nodes) < originalNodeCount {
				klog.V(1).Infof("Defrag: skipping atomic candidate - some nodes violated min quotas")
				recordAtomicGroupBlocked(filter, nodes)
				continue
			}
			candidate.Nodes = filter.filterNodesViolatingMinSize(p.ctx, nodes)
			if candidate.IsAtomic && len(candidate.Nodes) < originalNodeCount {
				klog.V(1).Infof("Defrag: skipping atomic candidate - some nodes violated min size")
				recordAtomicGroupBlocked(filter, candidate.Nodes)
				continue
			}
			candidate.Nodes = filter.filterNodesViolatingMaxDisruption(p.ctx, candidate.Nodes)
			if candidate.IsAtomic && len(candidate.Nodes) < originalNodeCount {
				klog.V(1).Infof("Defrag: skipping atomic candidate - some nodes violated max disruption")
				recordAtomicGroupBlocked(filter, candidate.Nodes)
				continue
			}
			if len(candidate.Nodes) == 0 {
				continue
			}

			candidate.Plugin = plugin
			return candidate, nil
		}
	}
	return nil, nil
}

// BestOptions filters expansion options that are acceptable for currently
// picked defrag Candidate. If no Candidate is picked for this loop it returns all
func (p *Processor) BestOptions(ctx context.Context, options []expander.Option, _ map[string]*framework.NodeInfo) []expander.Option {
	if p.pickedCandidateInfo == nil {
		return options
	}

	var validOptions []expander.Option
	for _, option := range options {
		if p.pickedCandidateInfo.candidate.Plugin.IsExpansionOptionValid(p.ctx, p.pickedCandidateInfo.candidate, option) {
			validOptions = append(validOptions, option)
		}
	}

	klog.V(2).Infof("Defrag kept %d out of %d scale-up options, plugin %q", len(validOptions), len(options), p.pickedCandidateInfo.candidate.Plugin.String())

	// Mark scale-up as impossible if there are no valid options
	p.pickedCandidateInfo.scaleUpNoOptions = len(validOptions) == 0

	return validOptions
}

// CleanUp cleans up the processor internal structures
func (p *Processor) CleanUp() {
}
