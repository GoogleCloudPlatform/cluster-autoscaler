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
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodes"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/fairness"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
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
	if info.candidate.Mode != defrag.Partial {
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
		nodeFilterFactory:  newDefragNodeFilterFactory(opts.ScaleDownNodeProcessor, opts.DeleteOptions, opts.DrainabilityRules, opts.MinQuotasTrackerFactory),
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
func (p *Processor) Process(ctx *cacontext.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	p.ctx = ctx
	p.pickedCandidateInfo = nil

	nodeFilter, err := p.nodeFilterFactory.NewDefragNodeFilter(p.ctx)
	if err != nil {
		return nil, err
	}

	if p.fairnessEnforcer != nil && !p.fairnessEnforcer.Admit(unschedulablePods) {
		return unschedulablePods, nil
	}

	if err := p.cleanUpCandidates(nodeFilter); err != nil {
		return nil, err
	}
	if err := p.nodeReconciler.Reconcile(ctx, p.candidateInfos); err != nil {
		return nil, err
	}
	pods, err := p.processCandidates(nodeFilter)
	if err != nil {
		return nil, err
	}

	klog.V(4).Infof("Defrag candidate count: %v", len(p.candidateInfos))

	if p.pickedCandidateInfo != nil {
		klog.V(4).Infof("Defrag candidate %v picked", p.pickedCandidateInfo)
		return pods, nil
	}

	klog.V(4).Infof("No defrag candidates, restoring unschedulable pods")
	return unschedulablePods, nil
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
		filter.filterInvalidCandidateNodes(p.ctx, p.pdbTracker, info.candidate)
		nodes, err := filter.filterNodesViolatingMinQuotas(p.ctx, info.candidate.Nodes)
		if err != nil {
			klog.Errorf("Defrag: failed to filter nodes violating min quotas for candidate %v: %v", info, err)
			return err
		}
		info.candidate.Nodes = filter.filterNodesViolatingMinSize(p.ctx, nodes)

		if shouldRemove, reason := p.shouldRemoveCandidate(info); shouldRemove {
			if reason == noValidNodes && isScaledDown {
				klog.V(4).Infof("Defrag candidate %v fully scaled down", info)
			} else {
				metrics.Metrics.IncrementDefragInvalidatedCandidatesTotal(string(reason), info.candidate.Plugin.String())
				klog.V(4).Infof("Defrag candidate %v no longer valid: %v", info, reason)
				p.backoff.backoff(p.ctx, info.candidate)
			}
			// The leftover nodes
			metrics.Metrics.IncreaseDefragFailedScaleDownNodesTotal(info.candidate.Plugin.String(), len(info.candidate.Nodes))
			continue
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
	for idx := 0; idx < p.config.CandidateLimit; idx++ {
		// Find a new candidate if needed
		if idx >= len(p.candidateInfos) {
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
			return unschedulablePods, nil
		}
	}

	return nil, nil
}

// processCandidate processes a single defrag Candidate and returns its unschedulable pods if any exist
func (p *Processor) processCandidate(info *candidateInfo, allCandidatesNodes map[string]bool) ([]*apiv1.Pod, error) {
	if info.candidate.Mode == defrag.Partial {
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
		if p.clock.Since(defragPossibleTime) >= p.config.ScaleDownDelay {
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
	nodeNames, err := filter.newValidCandidateNodes(p.ctx, p.pdbTracker, allCandidateNodes)
	if err != nil {
		return nil, err
	}

	for _, plugin := range p.plugins {
		availableNodes, backedOffNodes := p.backoff.splitNodesBasedOnBackoff(plugin, nodeNames)
		candidate := plugin.NewCandidate(p.ctx, availableNodes)
		unfitNodesCount := plugin.LatestUnfitNodesCount()
		metrics.Metrics.SetDefragUnfitNodes(plugin.String(), unfitNodesCount+len(backedOffNodes))
		metrics.Metrics.ObserveDefragStaleness(plugin.String())
		if candidate != nil {
			if !p.partialEnabled() && candidate.Mode == defrag.Partial {
				candidate.Mode = defrag.CreateBeforeDelete
				if len(candidate.Nodes) > 0 {
					candidate.Nodes = []string{candidate.Nodes[0]}
				}
			}
			nodes, err := filter.filterNodesViolatingMinQuotas(p.ctx, candidate.Nodes)
			if err != nil {
				klog.Errorf("Defrag: failed to filter nodes violating min quotas for plugin %s: %v", plugin.String(), err)
				continue
			}
			candidate.Nodes = filter.filterNodesViolatingMinSize(p.ctx, nodes)

			klog.V(4).Infof("Creating new defrag candidate for plugin: %s, nodes: %s", plugin.String(), strings.Join(candidate.Nodes, ","))
			pods, err := recreatablePods(p.ctx.ClusterSnapshot, candidate.Nodes)
			if err != nil {
				return nil, err
			}

			p.pdbTracker.RemovePods(pods)
			candidate.Plugin = plugin
			ci := &candidateInfo{candidate: candidate, creationTime: p.clock.Now()}
			if err := p.nodeReconciler.ReconcileCandidate(p.ctx, ci); err != nil {
				return nil, err
			}
			return ci, nil
		}
	}
	return nil, nil
}

// BestOptions filters expansion options that are acceptable for currently
// picked defrag Candidate. If no Candidate is picked for this loop it returns all
func (p *Processor) BestOptions(options []expander.Option, _ map[string]*framework.NodeInfo) []expander.Option {
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

func (p *Processor) partialEnabled() bool {
	if p.experimentsManager == nil {
		return false
	}
	return p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.EnablePartialDefragFlag, false)
}
