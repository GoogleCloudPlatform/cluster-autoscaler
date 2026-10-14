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
	"maps"
	"slices"
	"sort"
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// ScaleUpLimiterTracker tracks whether Flex Advisor constrained scale-up options during estimation.
type ScaleUpLimiterTracker interface {
	// MarkScaleUpOptionRemovedByFlexAdvisor records that a scale-up option was removed due to capacity constraints for the given node group and flexibility scope.
	MarkScaleUpOptionRemovedByFlexAdvisor(nodeGroupId string, flexibilityScope string)
	// GetFlexibilityScopesForNodeGroupIfRemoved returns a sorted list of flexibility scopes for which the specified node group had scale-up options removed during the current iteration.
	GetFlexibilityScopesForNodeGroupIfRemoved(nodeGroupId string) []string
	// GetFlexibilityScopesConstrainedByFlexAdvisor returns all flexibility scopes that had options removed in this scale up
	// TODO(b/570943323): CA will process one CCC at most during a scale up. We don't need to operate on arrays and "scopes" (as in, plural) processed during a loop
	GetFlexibilityScopesConstrainedByFlexAdvisor() []string
	// WasNodeGroupRemovedByFlexAdvisor returns true if the specified node group had scale-up options removed during the current iteration.
	WasNodeGroupRemovedByFlexAdvisor(nodeGroupId string) bool
	// Reset clears the tracked scale-up option removal state for the next evaluation iteration.
	Reset()
}

type nodeGroupId = string
type flexibilityScopeId = string

type scaleUpLimiterTracker struct {
	mu                    sync.RWMutex
	gceFlexAdvisorEnabled bool
	experimentsManager    experiments.Manager
	// removedNodeGroupsToScopes maps removed node groups to which scopes they used {nodeGroupId: {scope1: true scope2: true}}
	removedNodeGroupsToScopes map[nodeGroupId]map[flexibilityScopeId]bool
}

// IsFlexAdvisorScaleUpLimiterTrackerEnabled returns whether FlexAdvisor ScaleUpLimiterTracker is enabled.
func IsFlexAdvisorScaleUpLimiterTrackerEnabled(gceFlexAdvisorEnabled bool, manager experiments.Manager) bool {
	if !gceFlexAdvisorEnabled {
		return false
	}
	if manager == nil {
		return true
	}
	return IsFlexAdvisorProcessingEnabled(manager) &&
		manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorScaleUpLimiterTrackerMinCAVersionFlag, true)
}

// NewScaleUpLimiterTracker initializes and returns a thread-safe ScaleUpLimiterTracker.
func NewScaleUpLimiterTracker(gceFlexAdvisorEnabled bool, experimentsManager experiments.Manager) ScaleUpLimiterTracker {
	return &scaleUpLimiterTracker{
		gceFlexAdvisorEnabled:     gceFlexAdvisorEnabled,
		experimentsManager:        experimentsManager,
		removedNodeGroupsToScopes: make(map[string]map[string]bool),
	}
}

// MarkScaleUpOptionRemovedByFlexAdvisor records that a scale-up option was removed due to capacity constraints for the given node group and flexibility scope.
func (t *scaleUpLimiterTracker) MarkScaleUpOptionRemovedByFlexAdvisor(nodeGroupId string, flexibilityScope string) {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return
	}

	if nodeGroupId == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.removedNodeGroupsToScopes[nodeGroupId] == nil {
		t.removedNodeGroupsToScopes[nodeGroupId] = make(map[string]bool)
	}
	if flexibilityScope != "" {
		t.removedNodeGroupsToScopes[nodeGroupId][flexibilityScope] = true
	}
}

// GetFlexibilityScopesConstrainedByFlexAdvisor returns a sorted list of all flexibility scopes that had options removed in this scale up.
func (t *scaleUpLimiterTracker) GetFlexibilityScopesConstrainedByFlexAdvisor() []string {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	scopeSet := make(map[string]bool)
	for _, ngScopes := range t.removedNodeGroupsToScopes {
		maps.Copy(scopeSet, ngScopes)
	}
	scopes := slices.Collect(maps.Keys(scopeSet))
	sort.Strings(scopes)
	return scopes
}

// GetFlexibilityScopesForNodeGroupIfRemoved returns a sorted list of flexibility scopes for which the specified node group had scale-up options removed during the current iteration.
func (t *scaleUpLimiterTracker) GetFlexibilityScopesForNodeGroupIfRemoved(nodeGroupId string) []string {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	scopes := slices.Collect(maps.Keys(t.removedNodeGroupsToScopes[nodeGroupId]))
	sort.Strings(scopes)
	return scopes
}

// WasNodeGroupRemovedByFlexAdvisor returns true if the specified node group had scale-up options removed during the current iteration.
func (t *scaleUpLimiterTracker) WasNodeGroupRemovedByFlexAdvisor(nodeGroupId string) bool {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	_, removed := t.removedNodeGroupsToScopes[nodeGroupId]
	return removed
}

// Reset clears the tracked scale-up option removal state for the next evaluation iteration.
func (t *scaleUpLimiterTracker) Reset() {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removedNodeGroupsToScopes = make(map[string]map[string]bool)
}
