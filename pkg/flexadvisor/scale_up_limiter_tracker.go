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
	"sort"
	"sync"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// ScaleUpLimiterTracker tracks whether Flex Advisor constrained scale-up options during estimation.
type ScaleUpLimiterTracker interface {
	// MarkScaleUpOptionRemoved records that a scale-up option was removed due to capacity constraints for the given node group and flexibility scope.
	MarkScaleUpOptionRemoved(nodeGroupId string, flexibilityScope string)
	// GetFlexibilityScopesWithRemovedScaleUpOptions returns a sorted list of flexibility scopes that had scale-up options removed during the current iteration.
	GetFlexibilityScopesWithRemovedScaleUpOptions() []string
	// GetRemovedNodeGroupIds returns a sorted list of node group IDs that had scale-up options removed during the current iteration.
	GetRemovedNodeGroupIds() []string
	// HasRemovedScaleUpOptions returns true if any scale-up options were removed during the current evaluation iteration.
	HasRemovedScaleUpOptions() bool
	// Reset clears the tracked scale-up option removal state for the next evaluation iteration.
	Reset()
}

type scaleUpLimiterTracker struct {
	mu                           sync.RWMutex
	gceFlexAdvisorEnabled        bool
	experimentsManager           experiments.Manager
	constrainedFlexibilityScopes map[string]bool
	constrainedNodeGroupIds      map[string]bool
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
		gceFlexAdvisorEnabled:        gceFlexAdvisorEnabled,
		experimentsManager:           experimentsManager,
		constrainedFlexibilityScopes: make(map[string]bool),
		constrainedNodeGroupIds:      make(map[string]bool),
	}
}

// MarkScaleUpOptionRemoved records that a scale-up option was removed due to capacity constraints for the given node group and flexibility scope.
func (t *scaleUpLimiterTracker) MarkScaleUpOptionRemoved(nodeGroupId string, flexibilityScope string) {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return
	}
	if nodeGroupId == "" && flexibilityScope == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if flexibilityScope != "" {
		t.constrainedFlexibilityScopes[flexibilityScope] = true
	}
	if nodeGroupId != "" {
		t.constrainedNodeGroupIds[nodeGroupId] = true
	}
}

// HasRemovedScaleUpOptions returns true if any scale-up options were removed during the current evaluation iteration.
func (t *scaleUpLimiterTracker) HasRemovedScaleUpOptions() bool {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.constrainedFlexibilityScopes) > 0 || len(t.constrainedNodeGroupIds) > 0
}

// GetFlexibilityScopesWithRemovedScaleUpOptions returns a sorted list of flexibility scopes that had scale-up options removed during the current iteration.
// Technically, in a single CA loop we won't process more than one CCC, so the list should contain at most 1 item.
func (t *scaleUpLimiterTracker) GetFlexibilityScopesWithRemovedScaleUpOptions() []string {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	scopes := make([]string, 0, len(t.constrainedFlexibilityScopes))
	for scope := range t.constrainedFlexibilityScopes {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	return scopes
}

// GetRemovedNodeGroupIds returns a sorted list of node group IDs that had scale-up options removed during the current iteration.
func (t *scaleUpLimiterTracker) GetRemovedNodeGroupIds() []string {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	groupIds := make([]string, 0, len(t.constrainedNodeGroupIds))
	for groupId := range t.constrainedNodeGroupIds {
		groupIds = append(groupIds, groupId)
	}
	sort.Strings(groupIds)
	return groupIds
}

// Reset clears the tracked scale-up option removal state for the next evaluation iteration.
func (t *scaleUpLimiterTracker) Reset() {
	if !IsFlexAdvisorScaleUpLimiterTrackerEnabled(t.gceFlexAdvisorEnabled, t.experimentsManager) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.constrainedFlexibilityScopes = make(map[string]bool)
	t.constrainedNodeGroupIds = make(map[string]bool)
}
