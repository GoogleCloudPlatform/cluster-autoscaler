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
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const (
	refreshInterval           = 10 * time.Second
	keepAliveInterval         = 10 * time.Minute
	defaultMaxScopes          = 50
	defaultMaxInstanceConfigs = 200
	scopeStalenessThreshold   = 30 * time.Second
)

func formatBoolPtr(b *bool) string {
	if b == nil {
		return "nil"
	}
	return strconv.FormatBool(*b)
}

type flexAdvisorMetrics interface {
	IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, cccState metrics.CccState, isScaleUpAnyway *bool, keyGenerationState metrics.KeyGenerationState)
}

// InstanceConfigUnavailableReason describes why availability data could not be retrieved for an instanceConfigKey.
type InstanceConfigUnavailableReason int

const (
	InstanceConfigUnavailableReasonUnknown InstanceConfigUnavailableReason = iota
	// InstanceConfigUnavailableReasonInstanceConfigNotPresentInGceResponse indicates that the scope was refreshed successfully,
	// but the requested instanceConfigKey was not present in the GCE Flex Advisor response.
	InstanceConfigUnavailableReasonInstanceConfigNotPresentInGceResponse
	// InstanceConfigUnavailableReasonFetchError indicates that the scope has no fresh data because the last refresh attempt failed with an error.
	InstanceConfigUnavailableReasonFetchError
	// InstanceConfigUnavailableReasonTimeout indicates that the initial scope fetch did not complete within the requested timeout.
	InstanceConfigUnavailableReasonTimeout
	// InstanceConfigUnavailableReasonStale indicates that the scope's cached availability data has exceeded the staleness threshold.
	InstanceConfigUnavailableReasonStale
)

func (r InstanceConfigUnavailableReason) String() string {
	switch r {
	case InstanceConfigUnavailableReasonInstanceConfigNotPresentInGceResponse:
		return "InstanceConfigNotPresentInGceResponse"
	case InstanceConfigUnavailableReasonFetchError:
		return "FetchError"
	case InstanceConfigUnavailableReasonTimeout:
		return "Timeout"
	case InstanceConfigUnavailableReasonStale:
		return "Stale"
	default:
		return "Unknown"
	}
}

// InstanceConfigUnavailableError is returned by GetAvailabilitySnapshot and AwaitAvailabilitySnapshotWithTimeout
// when the flexibility scope does not contain usable availability data for the requested instanceConfigKey.
type InstanceConfigUnavailableError struct {
	reason                  InstanceConfigUnavailableReason
	flexibilityScopeKey     string
	instanceConfigKey       string
	lastSuccessfulRefreshAt time.Time
	stalenessDiff           time.Duration
	lastFetchError          error
}

func (e InstanceConfigUnavailableError) Error() string {
	switch e.reason {
	case InstanceConfigUnavailableReasonInstanceConfigNotPresentInGceResponse:
		return fmt.Sprintf("instanceConfigKey=%v not present in scope data, flexibilityScopeKey=%v", e.instanceConfigKey, e.flexibilityScopeKey)
	case InstanceConfigUnavailableReasonFetchError:
		return fmt.Sprintf("last refresh resulted in error, not using availability data, flexibilityScopeKey=%v, reason=%v, lastSuccessfulRefreshAt=%v, lastRefreshErr=%v", e.flexibilityScopeKey, e.reason, e.lastSuccessfulRefreshAt, e.lastFetchError)
	case InstanceConfigUnavailableReasonTimeout:
		return fmt.Sprintf("timeout when waiting for first fetch, no data available, flexibilityScopeKey=%v, reason=%v, lastSuccessfulRefreshAt=%v, lastRefreshErr=%v", e.flexibilityScopeKey, e.reason, e.lastSuccessfulRefreshAt, e.lastFetchError)
	case InstanceConfigUnavailableReasonStale:
		return fmt.Sprintf("scope is stale, not using availability data, flexibilityScopeKey=%v, reason=%v, lastSuccessfulRefreshAt=%v (diff: %v), lastRefreshErr=%v", e.flexibilityScopeKey, e.reason, e.lastSuccessfulRefreshAt, e.stalenessDiff, e.lastFetchError)
	default:
		return fmt.Sprintf("data not available, flexibilityScopeKey=%v, reason=%v, lastSuccessfulRefreshAt=%v (diff: %v), lastRefreshErr=%v", e.flexibilityScopeKey, e.reason, e.lastSuccessfulRefreshAt, e.stalenessDiff, e.lastFetchError)
	}
}

type flexAdvisor struct {
	rwMutex                 sync.RWMutex
	context                 context.Context
	scopes                  map[string]*flexibilityScope
	scopesActiveUntil       map[string]time.Time
	adviceProvider          api.AdviceProvider
	clock                   clock.Clock
	instanceConfigGenerator *instanceConfigGenerator
	cccLister               lister.Lister
	metrics                 flexAdvisorMetrics
	optionsTracker          *optstracking.OptionsTracker
	statusUpdatesCh         chan<- status.UpdateMessage
	bypassTracker           RecommendationsBypassTracker
}

type flexAdvisorConstructorOption func(*flexAdvisor)

// NewFlexAdvisor creates a new flexAdvisor object.
func NewFlexAdvisor(ctx context.Context, adviceProvider api.AdviceProvider, cccLister lister.Lister, instanceConfigCloudProvider instanceConfigCloudProvider, optionsTracker *optstracking.OptionsTracker, statusUpdatesCh chan<- status.UpdateMessage, bypassTracker RecommendationsBypassTracker, opts ...flexAdvisorConstructorOption) (*flexAdvisor, error) {
	if adviceProvider == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil flexadvisor.AdviceProvider")
	}
	if cccLister == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil lister.lister")
	}
	if instanceConfigCloudProvider == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil flexadvisor.instanceConfigCloudProvider")
	}

	ig := NewInstanceConfigGenerator(ctx, cccLister, instanceConfigCloudProvider, optionsTracker)

	f := &flexAdvisor{
		scopes:                  make(map[string]*flexibilityScope),
		scopesActiveUntil:       make(map[string]time.Time),
		context:                 ctx,
		adviceProvider:          adviceProvider,
		clock:                   clock.RealClock{},
		instanceConfigGenerator: ig,
		cccLister:               cccLister,
		metrics:                 metrics.Metrics,
		optionsTracker:          optionsTracker,
		statusUpdatesCh:         statusUpdatesCh,
		bypassTracker:           bypassTracker,
	}
	for _, opt := range opts {
		opt(f)
	}

	go wait.BackoffUntil(f.removeExpiredFlexibilityScopes, &backOffManager{f.clock, refreshInterval, nil}, true, f.context.Done())
	return f, nil
}

func (f *flexAdvisor) isRecommendationsEnforcementPaused(flexibilityScopeKey, instanceConfigKey string) bool {
	if f.bypassTracker != nil && f.bypassTracker.IsRecommendationsEnforcementPaused(flexibilityScopeKey) {
		klog.Infof("FlexAdvisor: recommendations enforcement is paused for flexibilityScopeKey=%v, skipping returning availability data for instanceConfigKey=%v", flexibilityScopeKey, instanceConfigKey)
		return true
	}
	return false
}

func (f *flexAdvisor) RegisterFlexibilityScope(flexibilityScopeKey string) error {
	_, err := f.getOrAddFlexibilityScope(flexibilityScopeKey, true)
	return err
}

// GetAvailabilitySnapshot returns a thread-safe snapshot of the availability data for the given
// flexibilityScopeKey and instanceConfigKey without blocking for an in-flight initial fetch.
func (f *flexAdvisor) GetAvailabilitySnapshot(flexibilityScopeKey, instanceConfigKey string) (*instanceavailability.Snapshot, error) {
	return f.AwaitAvailabilitySnapshotWithTimeout(flexibilityScopeKey, instanceConfigKey, 0)
}

// AwaitFlexibilityScopeWithTimeout blocks until the scope's first fetch finishes (up to timeout).
// Will trigger RegisterFlexibilityScope if flexibilityScopeKey is missing in Provider.
// Does not update keep-alive of an existing scope.
func (f *flexAdvisor) AwaitFlexibilityScopeWithTimeout(flexibilityScopeKey string, timeout time.Duration) error {
	_, err := f.awaitFirstFetch(flexibilityScopeKey, "", timeout)
	return err
}

// AwaitAvailabilitySnapshotWithTimeout blocks until the scope's first fetch finishes (up to timeout)
// and returns a thread-safe snapshot of the availability data for the given flexibilityScopeKey and instanceConfigKey.
func (f *flexAdvisor) AwaitAvailabilitySnapshotWithTimeout(flexibilityScopeKey, instanceConfigKey string, timeout time.Duration) (*instanceavailability.Snapshot, error) {
	scope, err := f.awaitFirstFetch(flexibilityScopeKey, instanceConfigKey, timeout)
	if err != nil {
		return nil, err
	}

	data := scope.getAvailabilityData(instanceConfigKey)
	if data == nil {
		return nil, InstanceConfigUnavailableError{
			reason:              InstanceConfigUnavailableReasonInstanceConfigNotPresentInGceResponse,
			flexibilityScopeKey: flexibilityScopeKey,
			instanceConfigKey:   instanceConfigKey,
		}
	}

	return data.NewSnapshot(), nil
}

func (f *flexAdvisor) awaitFirstFetch(flexibilityScopeKey, instanceConfigKey string, timeout time.Duration) (*flexibilityScope, error) {
	if f.isRecommendationsEnforcementPaused(flexibilityScopeKey, instanceConfigKey) {
		return nil, fmt.Errorf("recommendations enforcement is paused for flexibilityScopeKey=%v", flexibilityScopeKey)
	}

	scope, scopeFound, isAtCapacity := f.getScope(flexibilityScopeKey)
	if !scopeFound || scope == nil {
		if isAtCapacity {
			return nil, fmt.Errorf("failed to dynamically add scope, err=active scope limit of %d reached", f.getMaxScopes())
		}
		var addErr error
		scope, addErr = f.getOrAddFlexibilityScope(flexibilityScopeKey, false)
		if addErr != nil {
			return nil, fmt.Errorf("failed to dynamically add scope, err=%v", addErr)
		}
	}

	f.waitForFirstFetchWithTimeout(scope, timeout)

	fetchStats := scope.getFetchStats()

	var stalenessDiff time.Duration
	if !fetchStats.lastSuccessfulRefreshAt.IsZero() {
		stalenessDiff = f.clock.Since(fetchStats.lastSuccessfulRefreshAt)
		// Data has been fetched and is fresh enough
		if stalenessDiff <= f.getScopeStalenessThreshold() {
			return scope, nil
		}
	}
	if fetchStats.lastFetchError != nil {
		return nil, InstanceConfigUnavailableError{
			reason:                  InstanceConfigUnavailableReasonFetchError,
			flexibilityScopeKey:     flexibilityScopeKey,
			lastSuccessfulRefreshAt: fetchStats.lastSuccessfulRefreshAt,
			lastFetchError:          fetchStats.lastFetchError,
		}
	}
	if fetchStats.lastSuccessfulRefreshAt.IsZero() {
		return nil, InstanceConfigUnavailableError{
			reason:                  InstanceConfigUnavailableReasonTimeout,
			flexibilityScopeKey:     flexibilityScopeKey,
			lastSuccessfulRefreshAt: fetchStats.lastSuccessfulRefreshAt,
			lastFetchError:          fetchStats.lastFetchError,
		}
	}
	// There was no error during last fetch, but data is stale - this usually means there's bug in background worker timer/queue/worker is stuck at GCE query
	return nil, InstanceConfigUnavailableError{
		reason:                  InstanceConfigUnavailableReasonStale,
		flexibilityScopeKey:     flexibilityScopeKey,
		lastSuccessfulRefreshAt: fetchStats.lastSuccessfulRefreshAt,
		stalenessDiff:           stalenessDiff,
	}
}

// waitForFirstFetchWithTimeout blocks until the scope's first fetch finishes or timeout elapses
func (f *flexAdvisor) waitForFirstFetchWithTimeout(scope *flexibilityScope, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	// TODO(b/577169104): separate boolean for storing whether we had first fetch won't be necessary after switch to channels
	if scope.firstFetchFinished.Load() {
		return
	}
	waitStart := time.Now()
	defer func() {
		waitDuration := time.Since(waitStart)
		if waitDuration > 100*time.Millisecond {
			klog.Warningf("FlexAdvisor: waited %v for GCE Flex Advisor consultation, flexibilityScopeKey=%v", waitDuration, scope.flexibilityScopeKey)
		}
	}()
	firstFetchDone := make(chan struct{})
	go func() {
		scope.firstFetchWG.Wait()
		close(firstFetchDone)
	}()
	timer := f.clock.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-firstFetchDone:
	case <-timer.C():
		return
	}
}

// BumpScopeTtl extends the keep-alive of an existing scope by the regular TTL.
// Returns an error if the flexibility scope does not exist.
// Consumers that act on availability data (the balancer) must call it explicitly, as read methods
// (GetAvailabilitySnapshot, AwaitAvailabilitySnapshotWithTimeout) never extend a scope's keep-alive implicitly.
func (f *flexAdvisor) BumpScopeTtl(flexibilityScopeKey string) error {
	scopeTtl := f.getScopeTtl()

	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()

	if _, found := f.scopes[flexibilityScopeKey]; !found {
		return fmt.Errorf("flexibility scope not found for key: %s", flexibilityScopeKey)
	}
	f.scopesActiveUntil[flexibilityScopeKey] = f.clock.Now().Add(scopeTtl)
	return nil
}

func calculateRefreshInterval(manager experiments.Manager) time.Duration {
	if manager == nil {
		return refreshInterval
	}
	duration := manager.EvaluateDurationSecondsFlagOrFailsafe(
		experiments.FlexAdvisorScopeRefreshIntervalSecondsFlag,
		refreshInterval,
	)
	if duration <= 0 {
		return refreshInterval
	}
	return duration
}

func (f *flexAdvisor) getScopeStalenessThreshold() time.Duration {
	if f.optionsTracker == nil || f.optionsTracker.ExperimentsManager() == nil {
		return scopeStalenessThreshold
	}
	manager := f.optionsTracker.ExperimentsManager()
	refInterval := calculateRefreshInterval(manager)
	defaultThreshold := max(scopeStalenessThreshold, 3*refInterval)
	threshold := manager.EvaluateDurationSecondsFlagOrFailsafe(
		experiments.FlexAdvisorScopeStalenessThresholdSecondsFlag,
		defaultThreshold,
	)
	if threshold <= 0 {
		return defaultThreshold
	}
	return threshold
}

// IncrementFlexAdvisorCacheQueryCount gathers additional debugging info and calls underlying prometheus' IncrementFlexAdvisorCacheQueryCount
func (f *flexAdvisor) IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, flexibilityScopeKey string, instanceConfigKey string) {
	if result == metrics.FACacheHit {
		// we don't care for debugging data in case of HIT and gathering it may be costly
		f.metrics.IncrementFlexAdvisorCacheQueryCount(result, metrics.CccStateEmpty, nil, "")
		return
	}

	scope, _, _ := f.getScope(flexibilityScopeKey)

	cccState := f.cccState(scope)
	isScaleUpAnyway := f.isScaleUpAnyway(scope)
	keyGenerationState := f.keyGenerationState(scope, instanceConfigKey)

	f.metrics.IncrementFlexAdvisorCacheQueryCount(result, cccState, isScaleUpAnyway, keyGenerationState)
	switch result {
	case metrics.FACacheMissNoZone,
		metrics.FACacheMissNoInstanceConfigKey,
		metrics.FACacheMissFetchFailed:
		klog.Errorf("FlexAdvisor: reported cache miss flexibilityScopeKey=%v, result=%v, keyGenerationState=%v, cccState=%v, isScaleUpAnyway=%v", flexibilityScopeKey, result, keyGenerationState, cccState, formatBoolPtr(isScaleUpAnyway))
	default:
		klog.Warningf("FlexAdvisor: reported cache metric flexibilityScopeKey=%v, result=%v, keyGenerationState=%v, cccState=%v, isScaleUpAnyway=%v", flexibilityScopeKey, result, keyGenerationState, cccState, formatBoolPtr(isScaleUpAnyway))
	}
}

// cccState whether scope keys where generated using outdated CRD
func (f *flexAdvisor) cccState(scope *flexibilityScope) metrics.CccState {
	if scope == nil {
		return metrics.CccStateEmpty
	}
	if f.instanceConfigGenerator.isPayPerPodPodFamilyScope(scope.flexibilityScopeKey) {
		// Synthetic podFamily scopes are not backed by a CRD, so there is no resource version to compare against.
		return metrics.CccStateEmpty
	}
	crd, err := f.cccLister.GetCrd(scope.flexibilityScopeKey)
	if err != nil {
		klog.Errorf("FlexAdvisor: error getting crd flexibilityScopeKey=%v, err=%v", scope.flexibilityScopeKey, err)
		return metrics.CccStateEmpty
	}
	if crd == nil {
		return metrics.CccStateEmpty
	}
	generatedUsing := scope.getGeneratedUsing()
	if generatedUsing == nil {
		return metrics.CccStateEmpty
	}
	if crd.ResourceVersion() != generatedUsing.ResourceVersion() {
		return metrics.CccStateStale
	}
	return metrics.CccStateEmpty
}

// isScaleUpAnyway returns true/false based on CCC ScaleUpAnyway field, nil if cannot determine
func (f *flexAdvisor) isScaleUpAnyway(scope *flexibilityScope) *bool {
	if scope == nil {
		return nil
	}
	if f.instanceConfigGenerator.isPayPerPodPodFamilyScope(scope.flexibilityScopeKey) {
		// Synthetic podFamily scopes never use ScaleUpAnyway.
		return ptr.To(false)
	}
	crd, err := f.cccLister.GetCrd(scope.flexibilityScopeKey)
	if err != nil {
		klog.Errorf("FlexAdvisor: error getting crd flexibilityScopeKey=%v, err=%v", scope.flexibilityScopeKey, err)
		return nil
	}
	if crd == nil {
		// TODO(b/514250091): at the time of writing no PCC uses ScaleUpAnyway, we should query them here dynamically
		return ptr.To(false)
	}
	return ptr.To(crd.ScaleUpAnyway())
}

// keyGenerationState returns KeyGenerationState val for emitting flexadvisor_cache_query_count
func (f *flexAdvisor) keyGenerationState(scope *flexibilityScope, instanceConfigKey string) metrics.KeyGenerationState {
	if scope == nil {
		return ""
	}

	val, ok := scope.getKeyCapState(instanceConfigKey)
	if !ok {
		return metrics.KeyGenerationStateNotGenerated
	}
	if val {
		return metrics.KeyGenerationStateGeneratedButCapped
	}
	return metrics.KeyGenerationStateGeneratedAndSent
}

func (f *flexAdvisor) MarkUsed(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) error {
	scope, scopeFound, _ := f.getScope(flexibilityScopeKey)
	if !scopeFound || scope == nil {
		return fmt.Errorf("flexibility scope not found for key: %s", flexibilityScopeKey)
	}
	data := scope.getAvailabilityData(instanceConfigKey)
	if data == nil {
		return fmt.Errorf("availability data not found for flexibilityScopeKey=%s, instanceConfigKey=%s", flexibilityScopeKey, instanceConfigKey)
	}
	data.MarkUsed(zonalInstancesToProvision, decisionId, guidanceId)
	return nil
}

// removeExpiredFlexibilityScopes removes flexibility scopes from scopes if
// RegisterFlexibilityScope() for that flexibilityScope, is not called recently
// or the scope is not accessed recently.
func (f *flexAdvisor) removeExpiredFlexibilityScopes() {
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()
	now := f.clock.Now()
	for key, scope := range f.scopes {
		if f.scopeExpired(key, now) {
			klog.Infof("FlexAdvisor[async-worker]: scope flexibilityScopeKey=%v expired, removing from async refresh", scope.flexibilityScopeKey)
			scope.cancelFunc()
			delete(f.scopes, key)
			delete(f.scopesActiveUntil, key)
		}
	}
	metrics.Metrics.UpdateFlexAdvisorActiveScopes(len(f.scopes))
}

func (f *flexAdvisor) scopeExpired(key string, now time.Time) bool {
	expireTime, found := f.scopesActiveUntil[key]
	if !found {
		return false
	}
	return now.After(expireTime)
}

func (f *flexAdvisor) getMaxScopes() int {
	maxScopes := 0
	if f.optionsTracker != nil {
		maxScopes = f.optionsTracker.ExperimentsManager().EvaluateIntFlagOrFailsafe(experiments.FlexAdvisorMaxActiveScopes, defaultMaxScopes)
	}
	if maxScopes <= 0 {
		maxScopes = defaultMaxScopes
	}
	return maxScopes
}

func (f *flexAdvisor) getScope(flexibilityScopeKey string) (*flexibilityScope, bool, bool) {
	f.rwMutex.RLock()
	defer f.rwMutex.RUnlock()
	scope, scopeFound := f.scopes[flexibilityScopeKey]
	if scopeFound && scope != nil {
		return scope, true, false
	}

	maxScopes := f.getMaxScopes()
	isAtCapacity := len(f.scopes) >= maxScopes

	return scope, scopeFound, isAtCapacity
}

func (f *flexAdvisor) getScopeTtl() time.Duration {
	if f.optionsTracker == nil || f.optionsTracker.ExperimentsManager() == nil {
		return keepAliveInterval
	}
	duration := f.optionsTracker.ExperimentsManager().EvaluateDurationSecondsFlagOrFailsafe(
		experiments.FlexAdvisorScopeKeepAliveSecondsFlag,
		keepAliveInterval,
	)
	if duration <= 0 {
		return keepAliveInterval
	}
	return duration
}

// getOrAddFlexibilityScope returns the scope, registering it (which starts its background refresh) if missing.
// If the scope is already registered, its keep-alive is extended only when bumpScopeTtl is true.
// TODO(b/577169261): we should not have to upsert the scope if we remove the async expiration
func (f *flexAdvisor) getOrAddFlexibilityScope(flexibilityScopeKey string, bumpScopeTtl bool) (*flexibilityScope, error) {
	scopeTtl := f.getScopeTtl()

	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()

	scope, found := f.scopes[flexibilityScopeKey]
	if found {
		if bumpScopeTtl {
			f.scopesActiveUntil[flexibilityScopeKey] = f.clock.Now().Add(scopeTtl)
		}
		return scope, nil
	}

	maxScopes := f.getMaxScopes()

	if len(f.scopes) >= maxScopes {
		return nil, fmt.Errorf("active scope limit of %d reached", maxScopes)
	}
	ctx, cancel := context.WithCancel(f.context)
	scope = newFlexibilityScope(f, flexibilityScopeKey, cancel)

	f.scopes[flexibilityScopeKey] = scope
	f.scopesActiveUntil[flexibilityScopeKey] = f.clock.Now().Add(scopeTtl)

	metrics.Metrics.UpdateFlexAdvisorActiveScopes(len(f.scopes))

	worker := newScopeWorker(scope, f)

	go worker.run(ctx)
	klog.V(4).Infof("FlexAdvisor: Registered a new flexibility scope: %s", flexibilityScopeKey)

	return scope, nil
}

type backOffManager struct {
	clock        clock.Clock
	duration     time.Duration
	backoffTimer clock.Timer
}

// Backoff implements BackoffManager.Backoff, it returns a timer so caller can block on the timer for backoff.
func (b *backOffManager) Backoff() clock.Timer {
	if b.backoffTimer == nil {
		b.backoffTimer = b.clock.NewTimer(b.duration)
	} else {
		b.backoffTimer.Reset(b.duration)
	}
	return b.backoffTimer
}

func isFlexAdvisorDWSEnabled(manager experiments.Manager) bool {
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorDWSEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorDWSMinCAVersionFlag, false)
}

func isFlexAdvisorTPUEnabled(manager experiments.Manager) bool {
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorTPUEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorTPUMinCAVersionFlag, false)
}

// isFlexAdvisorZoneTypesEnabled separate control arm besides global g.optionsTracker.Options().ZoneTypesEnabled to control whether FA should process
// rule.ZoneTypes() in the generator.
func isFlexAdvisorZoneTypesEnabled(manager experiments.Manager) bool {
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorZoneTypesEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorZoneTypesMinCAVersionFlag, true)
}

func isFlexAdvisorMinCpuPlatformSupportEnabled(manager experiments.Manager) bool {
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorMinCpuPlatformEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorMinCpuPlatformMinCAVersionFlag, true)
}

// IsFlexAdvisorPCCSupportEnabled returns whether Flex Advisor support for Predefined Compute Classes is enabled.
func IsFlexAdvisorPCCSupportEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorPCCSupportEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorPCCSupportMinCAVersionFlag, true)
}

// FlexAdvisorProcessingEnabledFlag can be used to disable FA without restarting CAs
func IsFlexAdvisorProcessingEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorProcessingEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorProcessingMinCAVersionFlag, true)
}

// IsFlexAdvisorLateRegistrationEnabled returns whether FA late registration is enabled.
func IsFlexAdvisorLateRegistrationEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorLateRegistrationEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorLateRegistrationMinCAVersionFlag, true)
}

func isFlexAdvisorGeneratorMachineErrorsCacheEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorGeneratorMachineErrorsCacheEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorGeneratorMachineErrorsCacheMinCAVersionFlag, true)
}

func isFlexAdvisorDebugLogsEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return false
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorEnableDebugLogsFlag, false)
}

func isFlexAdvisorReservationSpecificMigsProcessingEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorEnableReservationSpecificMigsProcessingFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorEnableReservationSpecificMigsProcessingMinCAVersionFlag, true)
}

// IsFlexAdvisorNapZoneSetExpansionEnabled returns whether evaluating uncreated node pool MIGs across their planned zone set is enabled.
func IsFlexAdvisorNapZoneSetExpansionEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return false
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorNapZoneSetExpansionEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorNapZoneSetExpansionMinCAVersionFlag, false)
}

// isFlexAdvisorPodFamilySequentialOrderEnabled returns whether podFamily machine families get sequential sub-ranks (kill-switch, default on).
func isFlexAdvisorPodFamilySequentialOrderEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorPodFamilySequentialOrderEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorPodFamilySequentialOrderMinCAVersionFlag, true)
}

// isFlexAdvisorRecommendationsBypassEnabled returns whether recommendations bypass is enabled.
func isFlexAdvisorRecommendationsBypassEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return true
	}
	return IsFlexAdvisorProcessingEnabled(manager) &&
		IsFlexAdvisorScaleUpLimiterTrackerEnabled(true, manager) &&
		manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorRecommendationsBypassEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorRecommendationsBypassMinCAVersionFlag, true)
}

// IsFlexAdvisorBinpackerBlockEnabled returns whether bounded blocking wait in bin-packing is enabled.
func IsFlexAdvisorBinpackerBlockEnabled(manager experiments.Manager) bool {
	if manager == nil {
		return false
	}
	return IsFlexAdvisorProcessingEnabled(manager) &&
		manager.EvaluateBoolFlagOrFailsafe(experiments.FlexAdvisorBinpackerBlockEnabledFlag, true) &&
		manager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag, true)
}
