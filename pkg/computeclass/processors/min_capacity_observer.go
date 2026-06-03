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

package processors

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	cc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

const (
	longUnprovisionedThreshold = 30 * time.Minute
)

var (
	scaleUpConditions = []metav1.Condition{
		*status.MinCapacityProvisioningCondition(metav1.ConditionTrue, status.ProvisioningStarted, ""),
		*status.MinCapacityProvisionedCondition(metav1.ConditionFalse, status.ProvisioningInProgress, ""),
	}

	provisioningCompleteConditions = []metav1.Condition{
		*status.MinCapacityProvisionedCondition(metav1.ConditionTrue, status.ProvisioningComplete, ""),
	}

	provisioningCompleteRemoveConditions = []string{
		status.MinCapacityProvisioning,
	}
)

// minCapacityMetrics is an interface for emitting metrics related to minimum capacity provisioning.
type minCapacityMetrics interface {
	ObserveCcMinTargetNodesReactionLatency(definedInPriority bool, duration time.Duration)
	ObserveCcMinTargetNodesProvisioningLatency(provisioningErrorEncountered string, unhelpable, definedInPriority bool, duration time.Duration)
	ObserveCcLongUnprovisionedMinTargetNodesCount(samples []metrics.CcLongUnprovisionedSample)
}

// MinCapacityObserver tracks the status of minimum capacity provisioning and emits metrics.
type MinCapacityObserver interface {
	// OnScaleUpDecision is called each time a scale-up decision is made for a ComputeClass (CCC)
	// that is associated with a MinimumCapacity requirement.
	// It is used to measure the reaction latency (the time elapsed between detecting the shortfall
	// and deciding to scale up).
	OnScaleUpDecision(ccName string, ruleIdx int, now time.Time)

	// OnProvisioningComplete is called when the MinimumCapacity requirement (either at the spec level
	// or within a priority rule) is fully satisfied (i.e., target node count is met). It is used to
	// stop latency timers and record successful recovery.
	OnProvisioningComplete(ccName string, ruleIdx int, now time.Time)

	// OnProvisioningError is called when node provisioning fails or encounters errors
	// while trying to satisfy a MinimumCapacity requirement for a CCC. It records the error reason
	// and unhelpable state to be used in long-unprovisioned metrics.
	OnProvisioningError(ccName string, errType string, unhelpable bool, now time.Time)

	// CheckLongUnprovisioned checks all tracked ComputeClasses and should be called at regular intervals
	// It updates the cc_long_unprovisioned_min_target_nodes_count metric for targets that have remained
	// unsatisfied for longer than the threshold (e.g., 30 minutes).
	CheckLongUnprovisioned(now time.Time)

	// OnShortfallDetected is called when a shortfall (unsatisfied capacity relative to the target) is detected.
	// If the capacity requirement was previously satisfied, this indicates a capacity regression
	// (e.g., node termination or eviction) and resets timers to measure the recovery/replacement time.
	OnShortfallDetected(ccName string, ruleIdx int, now time.Time)

	// OnComputeClassAdded registers state tracking for a newly added ComputeClass CRD.
	OnComputeClassAdded(cc *cc_api.ComputeClass, now time.Time)

	// OnComputeClassUpdated resets state tracking for a ComputeClass if logically updated.
	OnComputeClassUpdated(oldCC, newCC *cc_api.ComputeClass, now time.Time)

	// OnComputeClassDeleted cleans up and removes state tracking for a deleted ComputeClass.
	OnComputeClassDeleted(name string)
}

// minCapacityObserver is the concrete implementation of MinCapacityObserver.
type minCapacityObserver struct {
	sync.Mutex
	metrics   minCapacityMetrics
	updatesCh chan status.UpdateMessage

	// Tracks all CCs state. CC Name -> State.
	ccStates map[string]*minCapacityProvisioningState
}

// minCapacityProvisioningState tracks the state of minimum capacity provisioning for a specific CC.
type minCapacityProvisioningState struct {
	// Spec level provisioning state
	spec *provisioningTargetState

	// Map of priority index to its provisioning state.
	priorities map[int]*provisioningTargetState
}

// provisioningTargetState tracks the lifecycle timers and error state for a specific minimum capacity target
// (either at the top-level ComputeClass specification or within an individual priority rule).
type provisioningTargetState struct {
	firstObservedAt            time.Time
	reactionLatencyEmitted     bool
	provisioningLatencyEmitted bool
	longUnprovisionedEmitted   bool

	lastError  string
	unhelpable bool
}

// NewMinCapacityObserver creates a new MinCapacityObserver.
// Note: To process CRD events, callers must explicitly connect this observer to an informer
// using SubscribeToComputeClassInformer.
func NewMinCapacityObserver(metrics minCapacityMetrics, updatesCh chan status.UpdateMessage) MinCapacityObserver {
	return &minCapacityObserver{
		metrics:   metrics,
		updatesCh: updatesCh,
		ccStates:  make(map[string]*minCapacityProvisioningState),
	}
}

// SubscribeToComputeClassInformer registers event handlers on the informer to update the MinCapacityObserver.
func SubscribeToComputeClassInformer(informer cache.SharedIndexInformer, observer MinCapacityObserver) {
	informer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(obj interface{}, isInInitialList bool) {
			cc, ok := obj.(*cc_api.ComputeClass)
			if !ok {
				return
			}
			observer.OnComputeClassAdded(cc, time.Now())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldCC, okOld := oldObj.(*cc_api.ComputeClass)
			newCC, okNew := newObj.(*cc_api.ComputeClass)
			if !okOld || !okNew {
				return
			}
			observer.OnComputeClassUpdated(oldCC, newCC, time.Now())
		},
		DeleteFunc: func(obj interface{}) {
			cc, ok := obj.(*cc_api.ComputeClass)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				cc, ok = tombstone.Obj.(*cc_api.ComputeClass)
				if !ok {
					return
				}
			}
			observer.OnComputeClassDeleted(cc.Name)
		},
	})
}

// OnScaleUpDecision updates min target nodes reaction latency.
func (o *minCapacityObserver) OnScaleUpDecision(ccName string, ruleIdx int, now time.Time) {
	if o.updateStateOnScaleUpDecision(ccName, ruleIdx, now) {
		o.updateConditionsOnProvisioningStarted(ccName, ruleIdx)
	}
}

func (o *minCapacityObserver) updateStateOnScaleUpDecision(ccName string, ruleIdx int, now time.Time) bool {
	o.Lock()
	defer o.Unlock()

	state, ok := o.ccStates[ccName]
	if !ok {
		return false
	}

	var tState *provisioningTargetState
	if ruleIdx != -1 {
		tState = state.priorities[ruleIdx]
	} else {
		tState = state.spec
	}

	if tState == nil {
		return false
	}

	definedInPriority := ruleIdx != -1
	if !tState.reactionLatencyEmitted && !tState.firstObservedAt.IsZero() {
		latency := now.Sub(tState.firstObservedAt)
		o.metrics.ObserveCcMinTargetNodesReactionLatency(definedInPriority, latency)
		tState.reactionLatencyEmitted = true
	}

	return true
}

// OnProvisioningComplete updates min target nodes provisioning latency.
func (o *minCapacityObserver) OnProvisioningComplete(ccName string, ruleIdx int, now time.Time) {
	if o.updateStateOnProvisioningComplete(ccName, ruleIdx, now) {
		o.updateConditionsOnProvisioningComplete(ccName, ruleIdx)
	}
}

func (o *minCapacityObserver) updateStateOnProvisioningComplete(ccName string, ruleIdx int, now time.Time) bool {
	o.Lock()
	defer o.Unlock()

	state, ok := o.ccStates[ccName]
	if !ok {
		return false
	}

	var tState *provisioningTargetState
	if ruleIdx != -1 {
		tState = state.priorities[ruleIdx]
	} else {
		tState = state.spec
	}

	if tState == nil || tState.firstObservedAt.IsZero() {
		return false
	}

	definedInPriority := ruleIdx != -1
	if !tState.reactionLatencyEmitted {
		latency := now.Sub(tState.firstObservedAt)
		o.metrics.ObserveCcMinTargetNodesReactionLatency(definedInPriority, latency)
		tState.reactionLatencyEmitted = true
	}

	if !tState.provisioningLatencyEmitted {
		latency := now.Sub(tState.firstObservedAt)
		o.metrics.ObserveCcMinTargetNodesProvisioningLatency(tState.lastError, tState.unhelpable, definedInPriority, latency)
		tState.provisioningLatencyEmitted = true
	}

	if tState.longUnprovisionedEmitted {
		tState.longUnprovisionedEmitted = false
		o.emitLongUnprovisioned()
	}

	return true
}

func (o *minCapacityObserver) emitLongUnprovisioned() {
	var samples []metrics.CcLongUnprovisionedSample
	for _, state := range o.ccStates {
		if state.spec != nil && state.spec.longUnprovisionedEmitted {
			samples = append(samples, metrics.CcLongUnprovisionedSample{
				ProvisioningErrorEncountered: state.spec.lastError,
				Unhelpable:                   state.spec.unhelpable,
				DefinedInPriority:            false,
			})
		}
		for _, pState := range state.priorities {
			if pState.longUnprovisionedEmitted {
				samples = append(samples, metrics.CcLongUnprovisionedSample{
					ProvisioningErrorEncountered: pState.lastError,
					Unhelpable:                   pState.unhelpable,
					DefinedInPriority:            true,
				})
			}
		}
	}
	o.metrics.ObserveCcLongUnprovisionedMinTargetNodesCount(samples)
}

// OnProvisioningError updates the error / unhelpable state for a CC and emits metrics.
func (o *minCapacityObserver) OnProvisioningError(ccName string, errType string, unhelpable bool, now time.Time) {
	hasSpecLevel, priorityIndices := o.updateStateOnProvisioningError(ccName, errType, unhelpable, now)
	if hasSpecLevel {
		o.updateConditionsOnProvisioningFailed(ccName, -1, errType)
	}
	for _, idx := range priorityIndices {
		o.updateConditionsOnProvisioningFailed(ccName, idx, errType)
	}
}

func (o *minCapacityObserver) updateStateOnProvisioningError(ccName string, errType string, unhelpable bool, now time.Time) (bool, []int) {
	o.Lock()
	defer o.Unlock()

	state, ok := o.ccStates[ccName]
	if !ok {
		return false, nil
	}

	hasSpecLevel := state.spec != nil
	if state.spec != nil {
		state.spec.lastError = errType
		state.spec.unhelpable = unhelpable
	}

	var priorityIndices []int
	for idx, pState := range state.priorities {
		priorityIndices = append(priorityIndices, idx)
		pState.lastError = errType
		pState.unhelpable = unhelpable
	}

	return hasSpecLevel, priorityIndices
}

// OnShortfallDetected handles a shortfall detected in ready nodes.
// If capacity was previously fully satisfied (provisioningLatencyEmitted == true),
// this indicates a capacity regression (e.g. node death), so we reset recovery timers.
func (o *minCapacityObserver) OnShortfallDetected(ccName string, ruleIdx int, now time.Time) {
	o.Lock()
	defer o.Unlock()

	state, ok := o.ccStates[ccName]
	if !ok {
		return
	}

	var tState *provisioningTargetState
	if ruleIdx != -1 {
		tState = state.priorities[ruleIdx]
	} else {
		tState = state.spec
	}

	if tState == nil {
		return
	}

	if tState.provisioningLatencyEmitted {
		tState.firstObservedAt = now
		tState.reactionLatencyEmitted = false
		tState.provisioningLatencyEmitted = false
		klog.Infof("MinCapacityObserver: Capacity regression detected for %s (rule %d), resetting recovery timers", ccName, ruleIdx)
	}
}

// CheckLongUnprovisioned updates long unprovisioned CCs.
func (o *minCapacityObserver) CheckLongUnprovisioned(now time.Time) {
	o.Lock()
	defer o.Unlock()

	changed := false
	for _, state := range o.ccStates {
		// Top level
		if state.spec != nil && !state.spec.provisioningLatencyEmitted && !state.spec.firstObservedAt.IsZero() && now.Sub(state.spec.firstObservedAt) > longUnprovisionedThreshold {
			if !state.spec.longUnprovisionedEmitted {
				state.spec.longUnprovisionedEmitted = true
				changed = true
			}
		}

		// Per priority
		for _, pState := range state.priorities {
			if !pState.provisioningLatencyEmitted && !pState.firstObservedAt.IsZero() && now.Sub(pState.firstObservedAt) > longUnprovisionedThreshold {
				if !pState.longUnprovisionedEmitted {
					pState.longUnprovisionedEmitted = true
					changed = true
				}
			}
		}
	}

	if changed {
		o.emitLongUnprovisioned()
	}
}

// OnComputeClassAdded handles the addition of a new ComputeClass. It initializes its provisioning
// state tracking if the ComputeClass has any target node count defined.
func (o *minCapacityObserver) OnComputeClassAdded(cc *cc_api.ComputeClass, now time.Time) {
	o.Lock()
	defer o.Unlock()

	if !o.needsTracking(cc) {
		return
	}

	name := cc.Name
	state := o.ccStates[name]
	if state == nil {
		state = o.initNewState()
		o.ccStates[name] = state
	}

	o.updateTimestamps(state, cc, now)
}

// OnComputeClassUpdated handles updates to an existing ComputeClass. It resets tracking state
// if logical changes occurred (e.g., spec level target count or priority properties).
func (o *minCapacityObserver) OnComputeClassUpdated(oldCC, newCC *cc_api.ComputeClass, now time.Time) {
	o.Lock()
	defer o.Unlock()

	if !o.needsTracking(newCC) {
		o.cleanupCC(newCC.Name)
		return
	}

	state := o.ccStates[newCC.Name]
	if state == nil {
		state = o.initNewState()
		o.ccStates[newCC.Name] = state
	} else {
		o.resetStateIfChangedLogically(state, oldCC, newCC)
	}

	o.updateTimestamps(state, newCC, now)
}

// OnComputeClassDeleted handles the deletion of a ComputeClass. It cleans up the tracking state.
func (o *minCapacityObserver) OnComputeClassDeleted(name string) {
	o.Lock()
	defer o.Unlock()
	o.cleanupCC(name)
}

// cleanupCC removes the tracking state for a ComputeClass and resets associated unprovisioned capacity metrics.
func (o *minCapacityObserver) cleanupCC(name string) {
	state, ok := o.ccStates[name]
	if !ok {
		return
	}

	needEmit := (state.spec != nil && state.spec.longUnprovisionedEmitted)
	for _, pState := range state.priorities {
		if pState.longUnprovisionedEmitted {
			needEmit = true
		}
	}
	delete(o.ccStates, name)
	if needEmit {
		o.emitLongUnprovisioned()
	}
}

// needsTracking returns true if the CC has any target node count defined (either at spec level or in any rule).
func (o *minCapacityObserver) needsTracking(cc *cc_api.ComputeClass) bool {
	if targetNodeCount(cc) != nil {
		return true
	}
	for _, p := range cc.Spec.Priorities {
		if priorityTargetNodeCount(p) != nil {
			return true
		}
	}
	return false
}

// initNewState initializes a new minCapacityProvisioningState for a CC.
func (o *minCapacityObserver) initNewState() *minCapacityProvisioningState {
	return &minCapacityProvisioningState{
		priorities: make(map[int]*provisioningTargetState),
	}
}

// resetStateIfChangedLogically checks if the CC has changed logically and resets the state if so.
func (o *minCapacityObserver) resetStateIfChangedLogically(state *minCapacityProvisioningState, oldCC, newCC *cc_api.ComputeClass) {
	currentPriorities := newCC.Spec.Priorities
	oldPriorities := oldCC.Spec.Priorities
	priorityChangedForSpec := len(oldPriorities) != len(currentPriorities)

	needEmit := false

	// Cleanup removed priority indices to prevent state leaks
	for k, pState := range state.priorities {
		if k >= len(currentPriorities) {
			if pState.longUnprovisionedEmitted {
				needEmit = true
			}
			delete(state.priorities, k)
		}
	}

	// Check for priority changes
	for idx, currentPriority := range currentPriorities {
		if idx < len(oldPriorities) {
			oldPriority := oldPriorities[idx]

			if !reflect.DeepEqual(oldPriority, currentPriority) {
				// Reset priority state
				if pState, ok := state.priorities[idx]; ok {
					if pState.longUnprovisionedEmitted {
						needEmit = true
					}
					delete(state.priorities, idx)
				}
				klog.Infof("MinCapacityObserver: Reset priority %d state for CC %s due to logical change", idx, newCC.Name)

				// Check if priority changed for spec-level reset.
				// We intentionally ignore changes to the priority's MinimumCapacity.
				// Priority minimum capacity changes should reset only the priority-level timers, not the spec-level timers.
				oldPForSpec := oldPriority
				currentPForSpec := currentPriority
				oldPForSpec.MinimumCapacity = nil
				currentPForSpec.MinimumCapacity = nil

				if !reflect.DeepEqual(oldPForSpec, currentPForSpec) {
					priorityChangedForSpec = true
				}
			}
		} else {
			// New rule added at the end. This counts as a change in priorities.
			priorityChangedForSpec = true
		}
	}

	// Check if spec-level target changed
	oldTarget := targetNodeCount(oldCC)
	newTarget := targetNodeCount(newCC)
	specTargetChanged := (oldTarget == nil) != (newTarget == nil) ||
		(oldTarget != nil && newTarget != nil && *oldTarget != *newTarget)

	if priorityChangedForSpec || specTargetChanged {
		if state.spec != nil {
			if state.spec.longUnprovisionedEmitted {
				needEmit = true
			}
			state.spec = nil
		}
		klog.Infof("MinCapacityObserver: Reset spec state for CC %s due to logical change", newCC.Name)
	}

	if needEmit {
		o.emitLongUnprovisioned()
	}
}

// updateTimestamps updates the first observed timestamps for spec and priorities.
func (o *minCapacityObserver) updateTimestamps(state *minCapacityProvisioningState, cc *cc_api.ComputeClass, now time.Time) {
	if targetNodeCount(cc) != nil {
		if state.spec == nil {
			state.spec = &provisioningTargetState{
				firstObservedAt: now,
			}
			klog.Infof("MinCapacityObserver: Set spec firstObservedAt for CC %s to %v", cc.Name, now)
		}
	}

	for idx, p := range cc.Spec.Priorities {
		if priorityTargetNodeCount(p) != nil {
			if _, ok := state.priorities[idx]; !ok {
				state.priorities[idx] = &provisioningTargetState{
					firstObservedAt: now,
				}
			}
		}
	}
}

// targetNodeCount returns the spec-level TargetNodeCount if defined, or nil otherwise.
func targetNodeCount(cc *cc_api.ComputeClass) *int {
	if cc.Spec.MinimumCapacity == nil {
		return nil
	}
	return cc.Spec.MinimumCapacity.TargetNodeCount
}

// priorityTargetNodeCount returns the priority-level TargetNodeCount if defined, or nil otherwise.
func priorityTargetNodeCount(p cc_api.Priority) *int {
	if p.MinimumCapacity == nil {
		return nil
	}
	return p.MinimumCapacity.TargetNodeCount
}

func (o *minCapacityObserver) updateConditionsOnProvisioningStarted(ccName string, ruleIdx int) {
	o.sendConditionUpdate(ccName, ruleIdx, scaleUpConditions, nil)
}

func (o *minCapacityObserver) updateConditionsOnProvisioningComplete(ccName string, ruleIdx int) {
	o.sendConditionUpdate(ccName, ruleIdx, provisioningCompleteConditions, provisioningCompleteRemoveConditions)
}

func (o *minCapacityObserver) updateConditionsOnProvisioningFailed(ccName string, ruleIdx int, errType string) {
	msg := fmt.Sprintf(status.MinimumCapacityProvisioningFailedMessage, errType)
	conds := []metav1.Condition{
		*status.MinCapacityProvisioningCondition(metav1.ConditionFalse, status.ProvisioningFailed, msg),
		*status.MinCapacityProvisionedCondition(metav1.ConditionFalse, status.ProvisioningFailed, msg),
	}
	o.sendConditionUpdate(ccName, ruleIdx, conds, nil)
}

func (o *minCapacityObserver) sendConditionUpdate(ccName string, ruleIdx int, conds []metav1.Condition, removeConds []string) {
	if o.updatesCh == nil {
		return
	}

	id := status.CRDId{
		CRDName:  ccName,
		CRDLabel: gkelabels.ComputeClassLabel,
	}

	if ruleIdx != -1 {
		ruleIdxStr := fmt.Sprintf("%d", ruleIdx)
		select {
		case o.updatesCh <- status.UpdateMessage{
			Id: id,
			Mutate: func(s crd.CRDStatus) {
				existing := s.GetRuleConditions(ruleIdxStr)
				for _, nc := range conds {
					meta.SetStatusCondition(&existing, nc)
				}
				for _, rc := range removeConds {
					meta.RemoveStatusCondition(&existing, rc)
				}
				s.UpdateRuleConditions(ruleIdxStr, existing)
			},
		}:
		default:
			klog.Warningf("updatesCh is full, dropping status update for ComputeClass %s, rule %s", ccName, ruleIdxStr)
		}
	} else {
		select {
		case o.updatesCh <- status.UpdateMessage{
			Id: id,
			Mutate: func(s crd.CRDStatus) {
				existing := s.GetConditions()
				for _, nc := range conds {
					meta.SetStatusCondition(&existing, nc)
				}
				for _, rc := range removeConds {
					meta.RemoveStatusCondition(&existing, rc)
				}
				s.UpdateConditions(existing)
			},
		}:
		default:
			klog.Warningf("updatesCh is full, dropping status update for ComputeClass %s", ccName)
		}
	}
}
