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

package churntracker

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// backoffData is the lookahead backoff of a workload separation.
type backoffData struct {
	// tier is the 1-based index of the backoff duration in the BackoffDurations list.
	tier int
	// endTime is the time the backoff of the current tier ends.
	endTime time.Time
}

// backoffEvents are the lookahead backoffs per workload separation of a machine family.
type backoffEvents map[workloadKey]backoffData

type backoffTierKey struct {
	machineFamily string
	tier          int
}

// BackoffManager decides which workload separations should have their lookahead backed off, based on their churn events.
//
// A backoff is only ever started by reaching ChurnThreshold churn events within ChurnWindow. The first backoff of a
// workload separation uses the tier 1 duration. After a backoff ends, its tier is remembered: reaching ChurnThreshold
// again (counting only the churn events since the end of the backoff) starts a backoff of the next tier. The tier is
// reset once ResetWindow has passed since the end of the backoff and the separation had at most ResetMaxChurnEvents
// churn events within the last ResetWindow, so that its next backoff starts with tier 1 again.
// It is thread-safe.
type BackoffManager struct {
	clock    clock.PassiveClock
	provider ConfigProvider
	metrics  churnMetrics

	mu       sync.RWMutex
	backoffs backoffEvents
	// reportedTiers are the backoff tiers with workload separations reported by the last updateMetrics call, used to
	// report 0 for the tiers without workload separations anymore.
	reportedTiers sets.Set[backoffTierKey]
}

// NewBackoffManager returns a new initialized BackoffManager.
func NewBackoffManager(c clock.PassiveClock, provider ConfigProvider, metrics churnMetrics) *BackoffManager {
	if c == nil {
		c = clock.RealClock{}
	}
	return &BackoffManager{
		clock:    c,
		provider: provider,
		metrics:  metrics,
		backoffs: make(backoffEvents),
	}
}

// UpdateEvents starts new backoffs and escalates the ended ones based on the given churn events. It is expected to be
// called whenever new churn events are recorded, as starting and escalating a backoff can only be caused by them.
// The events are only read during the call and are not retained.
// It is a no-op if the experiment is disabled.
func (m *BackoffManager) UpdateEvents(events churnEvents, now time.Time) {
	if !m.isEnabled() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.resetBackoffs(events, now)
	m.createOrUpdateBackoffEvents(events, now)
	m.updateMetrics(now)
}

func (m *BackoffManager) ResetBackoffs(events churnEvents, now time.Time) {
	if !m.isEnabled() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.resetBackoffs(events, now)
	m.updateMetrics(now)
}

// IsBackedOff returns true if the lookahead of the workload separation of the machine family is backed off.
// Ended backoffs are only escalated by UpdateEvents or reset by UpdateEvents and ResetBackoffs, so the map can contain
// already ended backoffs.
func (m *BackoffManager) IsBackedOff(machineFamily, workloadID string) bool {
	if !m.isEnabled() {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	backoff, found := m.backoffs[workloadKey{machineFamily: machineFamily, workloadID: workloadID}]
	return found && m.clock.Now().Before(backoff.endTime)
}

// isEnabled indicates if the churn circuit breaker experiment is enabled.
func (m *BackoffManager) isEnabled() bool {
	return m.provider != nil && m.provider.IsEnabled()
}

// resetBackoffs removes the stale backoffs.
// Active backoffs and the ended ones that are not reset are kept, the latter to remember their tier.
// Note: churn events older than ChurnWindow are cleaned up by the churn tracker, so ResetWindow cannot be greater than ChurnWindow.
func (m *BackoffManager) resetBackoffs(events churnEvents, now time.Time) {
	for workloadKey, backoff := range m.backoffs {
		if !m.shouldReset(events[workloadKey], backoff, now) {
			continue
		}
		klog.V(4).Infof("%s: Reset lookahead backoff of workload ID %q of machine family %q (latest tier was %v)",
			logPrefix, workloadKey.workloadID, workloadKey.machineFamily, backoff.tier)
		delete(m.backoffs, workloadKey)
	}
}

// createOrUpdateBackoffEvents starts or escalates the backoffs of the workload separations that have at least ChurnThreshold
// churn events within ChurnWindow:
//   - without backoff: a tier 1 backoff is started.
//   - with an ended backoff: the backoff is restarted with the next tier. Only the churn events since the end of the
//     backoff are counted, so that the ones that started it are not counted again.
//   - with an active backoff: the backoff is kept as is.
//
// Note: It is expected to be called after resetBackoffs, so that the reset workload separations start with tier 1.
func (m *BackoffManager) createOrUpdateBackoffEvents(events churnEvents, now time.Time) {
	churnWindowStart := now.Add(-m.provider.ChurnWindow())
	for workloadKey, eventsForWorkloadKey := range events {
		tier, windowStart := 1, churnWindowStart
		if backoff, found := m.backoffs[workloadKey]; found {
			// Keep all active backoffs as is.
			if now.Before(backoff.endTime) {
				continue
			}
			tier = backoff.tier + 1
			windowStart = later(churnWindowStart, backoff.endTime)
		}
		if !m.shouldTriggerBackoff(eventsForWorkloadKey, windowStart) {
			continue
		}
		tier, duration := m.durationForTier(tier)
		backoff := backoffData{tier: tier, endTime: now.Add(duration)}
		m.backoffs[workloadKey] = backoff
		klog.V(4).Infof("%s: Backed off lookahead of workload ID %q of machine family %q with tier %d until %v",
			logPrefix, workloadKey.workloadID, workloadKey.machineFamily, backoff.tier, backoff.endTime)
	}
}

// durationForTier returns the backoff duration of the given 1-based tier.
// The tier is expected to be within [1, len(BackoffDurations)].
func (m *BackoffManager) durationForTier(tier int) (int, time.Duration) {
	durations := m.provider.BackoffDurations()
	tier = min(tier, len(durations))
	return tier, durations[tier-1]
}

// updateMetrics reports the number of workload separations per machine family and backoff tier whose lookahead is
// backed off at now, i.e. only the active backoffs. Ended backoffs that are not reset yet are not counted, as their
// lookahead is not suppressed anymore (IsBackedOff returns false). The tiers without active backoffs anymore (e.g.
// ended, reset or escalated) since the last call are reported as 0.
func (m *BackoffManager) updateMetrics(now time.Time) {
	if m.metrics == nil {
		return
	}
	counts := make(map[backoffTierKey]int)
	for workloadKey, backoff := range m.backoffs {
		if !now.Before(backoff.endTime) {
			continue
		}
		counts[backoffTierKey{machineFamily: workloadKey.machineFamily, tier: backoff.tier}]++
	}
	for key := range m.reportedTiers {
		if _, found := counts[key]; !found {
			m.metrics.UpdateLookaheadBackedOffWorkloadSeparations(key.machineFamily, key.tier, 0)
		}
	}
	reportedTiers := sets.New[backoffTierKey]()
	for key, count := range counts {
		m.metrics.UpdateLookaheadBackedOffWorkloadSeparations(key.machineFamily, key.tier, count)
		reportedTiers.Insert(key)
	}
	m.reportedTiers = reportedTiers
}

// shouldReset indicates if the ended backoff should be reset:
// Two conditions must hold for this:
// - backoff has expired (now >= endTime + resetWindow);
// - we had at most resetMaxChurnEvents churn events in [now-ResetWindow, now).
// Note: shouldReset does not take into account churn events that are happening now.
// This is done on purpose to clean up the entry first if it is eligible and apply the new first tier backoff if needed.
func (m *BackoffManager) shouldReset(events []time.Time, backoff backoffData, now time.Time) bool {
	resetWindow := m.provider.ResetWindow()
	backoffExpirationTime := backoff.endTime.Add(resetWindow)
	if now.Before(backoffExpirationTime) {
		return false
	}
	resetWindowStart := now.Add(-resetWindow)
	return countEventsBetween(events, resetWindowStart, now) <= m.provider.ResetMaxChurnEvents()
}

// shouldTriggerBackoff indicates if we need to trigger a backoff for the workload separation.
// The only condition is that it has at least ChurnThreshold churn events within ChurnWindow.
func (m *BackoffManager) shouldTriggerBackoff(events []time.Time, windowStart time.Time) bool {
	return countEventsAfterTs(events, windowStart) >= m.provider.ChurnThreshold()
}

// later returns the later of the two times.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
