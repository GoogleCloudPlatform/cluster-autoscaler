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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const defaultBlocksQtyThreshold = 3
const defaultMeasurementWindowMinutes = 60

// RecommendationsBypassTracker tracks consecutive scale-up failures for recommendations bypass.
type RecommendationsBypassTracker interface {
	// PauseRecommendationsEnforcement pauses FlexAdvisor recommendations enforcement (installs a bypass)
	// for the given flexibility scope and clears its blocked scale-up counter.
	// Should be called at the beginning of a scale-up pass (in WrapperOrchestrator.ScaleUp) when
	// HasReachedBypassThreshold returns true for the scope.
	PauseRecommendationsEnforcement(flexibilityScopeKey string)
	// RemoveBypasses cleans up all active recommendations enforcement bypasses installed in previous scale-up runs.
	// Should be called at the beginning of each scale-up pass (in WrapperOrchestrator.ScaleUp) before
	// evaluating whether new bypasses should be installed for the current pass.
	RemoveBypasses()
	// IsRecommendationsEnforcementPaused returns true if recommendations enforcement is currently paused (bypassed)
	// for the given flexibility scope.
	// Should be called when querying instance availability (flexAdvisor.GetInstanceAvailability and
	// flexAdvisor.AwaitInstanceAvailability) to skip FlexAdvisor enforcement during an active bypass.
	IsRecommendationsEnforcementPaused(flexibilityScopeKey string) bool
	// HasReachedBypassThreshold returns true if the scope has been blocked enough times within the recent
	// measurement window to be eligible for a recommendations bypass.
	// Should be called at the beginning of a scale-up pass (in WrapperOrchestrator.ScaleUp) to decide whether
	// to call PauseRecommendationsEnforcement for the scope.
	HasReachedBypassThreshold(flexibilityScopeKey string) bool
	// MarkBlocked records a blocked scale-up timestamp for each of the given flexibility scopes.
	// Should be called at the end of a scale-up pass (in WrapperOrchestrator.ScaleUp) when scale-up
	// finishes with ScaleUpNoOptionsAvailable for scopes that were constrained by FlexAdvisor.
	MarkBlocked(flexibilityScopeKeys []string)
	// ResetCounters resets blocked scale-up threshold counters for the given scopes.
	// Should be called at the end of a scale-up pass (in WrapperOrchestrator.ScaleUp) when scale-up
	// finishes with ScaleUpSuccessful for the active scope.
	ResetCounters(flexibilityScopeKeys []string)
}

type recommendationsBypassTracker struct {
	rwMutex            sync.RWMutex
	experimentsManager experiments.Manager
	// blockedScaleUpsTimestamps maps a flexibilityScopeKey (string) to a slice of timestamps ([]time.Time)
	// recording when scale-up runs were blocked by FlexAdvisor for that scope within the measurement window.
	blockedScaleUpsTimestamps *cache.Expiring
	// pausedRecommendationsEnforcement maps a flexibilityScopeKey (string) to a boolean (bool) indicating
	// whether FlexAdvisor recommendations enforcement is currently paused (bypassed) for that scope, with a 1h fallback TTL.
	pausedRecommendationsEnforcement *cache.Expiring
}

// NewRecommendationsBypassTracker creates a new RecommendationsBypassTracker.
func NewRecommendationsBypassTracker(experimentsManager experiments.Manager) RecommendationsBypassTracker {
	return &recommendationsBypassTracker{
		experimentsManager:               experimentsManager,
		blockedScaleUpsTimestamps:        cache.NewExpiring(),
		pausedRecommendationsEnforcement: cache.NewExpiring(),
	}
}

func (t *recommendationsBypassTracker) getMeasurementWindow() time.Duration {
	if t.experimentsManager == nil {
		return defaultMeasurementWindowMinutes * time.Minute
	}
	minutes := t.experimentsManager.EvaluateIntFlagOrFailsafe(experiments.FlexAdvisorRecommendationsBypassMeasurementWindowFlag, defaultMeasurementWindowMinutes)
	if minutes <= 0 {
		return defaultMeasurementWindowMinutes * time.Minute
	}
	return time.Duration(minutes) * time.Minute
}

// getBlockedScaleUpsThreshold returns how many scale ups must be blocked within measurement window to trigger recommendations bypass
func (t *recommendationsBypassTracker) getBlockedScaleUpsThreshold() int {
	if t.experimentsManager == nil {
		return defaultBlocksQtyThreshold
	}
	qty := t.experimentsManager.EvaluateIntFlagOrFailsafe(experiments.FlexAdvisorRecommendationsBypassCountThresholdFlag, defaultBlocksQtyThreshold)
	if qty <= 0 {
		return defaultBlocksQtyThreshold
	}
	return qty
}

// HasReachedBypassThreshold returns true if scope has been blocked enough in recent time to be eligible for bypass.
func (t *recommendationsBypassTracker) HasReachedBypassThreshold(flexibilityScopeKey string) bool {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return false
	}

	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

	now := time.Now()
	windowDuration := t.getMeasurementWindow()
	cutoff := now.Add(-windowDuration)
	validCount := 0
	if timestamps, ok := t.blockedScaleUpsTimestamps.Get(flexibilityScopeKey); ok {
		// TODO(b/570943323): iterating from back would seem more optimal?
		for _, blockTime := range timestamps.([]time.Time) {
			if blockTime.After(cutoff) {
				validCount++
			}
		}
	}

	return validCount >= t.getBlockedScaleUpsThreshold()
}

func (t *recommendationsBypassTracker) MarkBlocked(flexibilityScopeKeys []string) {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return
	}

	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	now := time.Now()
	windowDuration := t.getMeasurementWindow()
	// Remove blocks outside of measurement window when adding a block
	// TODO(b/570943323): if there's a bug that we are not installing bypass/triggering ResetCounters in timely manner
	// we may accumulate lots of blocks here leading to iterating over all blocks on and on just to append back the same elements
	// More optimal way (possibly) would be to remove elements from from until element is within window & break
	cutoff := now.Add(-windowDuration)
	// TODO(b/570943323): currently it's possible if there are duplicates in flexibilityScopeKeys we will double count block. In general flexibilityScopeKeys should not be an array
	// but instead just a string, because CA processes max 1 scope per loop
	for i := range flexibilityScopeKeys {
		flexibilityScopeKey := flexibilityScopeKeys[i]
		var validBlocks []time.Time
		if timestamps, ok := t.blockedScaleUpsTimestamps.Get(flexibilityScopeKey); ok {
			for _, blockTime := range timestamps.([]time.Time) {
				if blockTime.After(cutoff) {
					validBlocks = append(validBlocks, blockTime)
				}
			}
		}
		validBlocks = append(validBlocks, now)
		// TTL = t.window: if no new block happens for 1h, all timestamps in `valid`
		// are >1h old anyway, so cache.Expiring can safely evict the key.
		t.blockedScaleUpsTimestamps.Set(flexibilityScopeKey, validBlocks, windowDuration)
	}
}

// ResetCounters resets blocked scale-up threshold counters for the given scopes.
func (t *recommendationsBypassTracker) ResetCounters(flexibilityScopeKeys []string) {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return
	}

	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	for i := range flexibilityScopeKeys {
		flexibilityScopeKey := flexibilityScopeKeys[i]
		t.blockedScaleUpsTimestamps.Delete(flexibilityScopeKey)
	}
}

func (t *recommendationsBypassTracker) PauseRecommendationsEnforcement(flexibilityScopeKey string) {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return
	}

	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	// Pause state is short-lived (single scale-up pass, installed at the start of WrapperOrchestrator.ScaleUp)
	// but we set a safe 1h TTL just in case.
	// Also clear blockedScaleUpsTimestamps for this scope so that after this pass's
	// bypass is cleaned up by RemoveBypasses(), enforcement resumes until the threshold is reached again.
	t.pausedRecommendationsEnforcement.Set(flexibilityScopeKey, true, 1*time.Hour)
	t.blockedScaleUpsTimestamps.Delete(flexibilityScopeKey)
}

// RemoveBypasses cleans up all active recommendations enforcement bypasses installed in previous scale-up runs
// by replacing pausedRecommendationsEnforcement with a fresh cache.Expiring instance.
// Because cache.Expiring is a passive in-memory struct (no background goroutines or timers), overwriting it
// allows Go's garbage collector to immediately reclaim both the internal map and the min-heap entries.
func (t *recommendationsBypassTracker) RemoveBypasses() {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return
	}

	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	// TODO(b/570943323): following line will execute each loop. Due to GC operating with delay we may be thrashing objects unnecessarily
	// We should just gather keys that have recommendations paused and delete based on that array
	t.pausedRecommendationsEnforcement = cache.NewExpiring()
}

func (t *recommendationsBypassTracker) IsRecommendationsEnforcementPaused(flexibilityScopeKey string) bool {
	if !isFlexAdvisorRecommendationsBypassEnabled(t.experimentsManager) {
		return false
	}

	t.rwMutex.RLock()
	defer t.rwMutex.RUnlock()

	if paused, ok := t.pausedRecommendationsEnforcement.Get(flexibilityScopeKey); ok {
		return paused.(bool)
	}
	return false
}
