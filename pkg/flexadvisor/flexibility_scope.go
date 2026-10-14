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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"
)

const provisioningDecisionNotificationChanSize = 10

// PollingMode is an alias for instanceavailability.PollingMode.
type PollingMode = instanceavailability.PollingMode

const (
	PollingModeNormal = instanceavailability.PollingModeNormal
	PollingModeSlow   = instanceavailability.PollingModeSlow
)

type flexibilityScope struct {
	provider                instanceavailability.Provider
	flexibilityScopeKey     string
	experimentsManager      experiments.Manager
	instanceConfigs         map[string]*api.InstanceAvailability
	mutex                   sync.RWMutex
	firstFetchWG            sync.WaitGroup
	firstFetchFinished      atomic.Bool
	pollingMode             atomicValue[PollingMode]
	pollingModeCh           chan struct{}
	cancelFunc              context.CancelFunc
	lastErr                 error
	lastSuccessfulRefreshAt time.Time
	lastRefreshAttemptAt    time.Time
	// cappedKeysMap contains all generated keys:
	// - If a key was capped locally (not sent to the backend), its value is true.
	// - If a key was sent to the backend, its value is false.
	// Keys that were not generated at all (filtered out by rules) are not in this map.
	// Keys that were sent (value is false) but are missing from instanceConfigs were dropped by the backend.
	cappedKeysMap  map[string]bool
	generatedUsing crd.CRD
}

type scopeWorker struct {
	scope                    *flexibilityScope
	provisioningDecisionChan chan api.ProvisioningDecisionNotification
	adviceProvider           api.AdviceProvider
	clock                    clock.Clock
	instanceConfigGenerator  *instanceConfigGenerator
	experimentsManager       experiments.Manager
	cccLister                lister.Lister
	statusUpdatesCh          chan<- status.UpdateMessage
}

func newScopeWorker(scope *flexibilityScope, f *flexAdvisor) *scopeWorker {
	var experimentsManager experiments.Manager
	if f.optionsTracker != nil {
		experimentsManager = f.optionsTracker.ExperimentsManager()
	}
	return &scopeWorker{
		scope:                    scope,
		provisioningDecisionChan: make(chan api.ProvisioningDecisionNotification, provisioningDecisionNotificationChanSize),
		adviceProvider:           f.adviceProvider,
		clock:                    f.clock,
		instanceConfigGenerator:  f.instanceConfigGenerator,
		experimentsManager:       experimentsManager,
		cccLister:                f.cccLister,
		statusUpdatesCh:          f.statusUpdatesCh,
	}
}

func stopTimer(timer clock.Timer) {
	if !timer.Stop() {
		// Pre 1.23 drain was needed because timer.Stop() may have left dangling events.
		// On Go 1.23+, drain is still needed because k8s.io/utils/clock/testing.FakeClock uses a buffered channel
		// whose Stop/Reset does not drain unread ticks, so an explicit non-blocking drain is required. If we get rid of
		// FakeClock we should be able to just use timer.Stop()
		select {
		case <-timer.C():
		default:
		}
	}
}

func (w *scopeWorker) run(ctx context.Context) {
	w.refreshScope(ctx)
	w.scope.firstFetchFinished.Store(true)
	w.scope.firstFetchWG.Done()

	timer := w.clock.NewTimer(w.currentRefreshInterval())
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		w.drainPollingModeCh()
		// if previous loop ended not because of timer, make sure to reset and drain it
		stopTimer(timer)
		// Make sure since last refresh try (successful or not) it hasn't elapsed more than expected refresh interval (starvation bug)
		if w.clock.Since(w.scope.getLastRefreshAttemptAt()) >= w.currentRefreshInterval() {
			w.refreshScope(ctx)
		}

		elapsedSinceLastAttempt := w.currentRefreshInterval() - w.clock.Since(w.scope.getLastRefreshAttemptAt())
		// 1 second is used as lower bound instead of 0 to always have some space between subsequent attempts in case
		// there's issue with our elapsedSinceLastAttempt calculations
		timer.Reset(max(1*time.Second, elapsedSinceLastAttempt))

		select {
		case <-timer.C():
			continue
		case <-w.scope.pollingModeCh:
			if !IsFlexAdvisorAdaptivePollingEnabled(w.experimentsManager) {
				continue
			}
			// Potentially do faster `usualInterval * 0.5` refresh if we switched to fast polling
			if w.scope.getPollingMode() == PollingModeNormal && w.clock.Since(w.scope.getLastRefreshAttemptAt()) >= w.currentRefreshInterval()/2 {
				w.refreshScope(ctx)
			}
		case decision := <-w.provisioningDecisionChan:
			w.sendProvisioningDecision(ctx, decision)
		case <-ctx.Done():
			return
		}
	}
}

func (w *scopeWorker) currentRefreshInterval() time.Duration {
	if IsFlexAdvisorAdaptivePollingEnabled(w.experimentsManager) {
		return getRefreshIntervalForPollingMode(w.experimentsManager, w.scope.getPollingMode())
	}
	return getRefreshIntervalForPollingMode(w.experimentsManager, PollingModeNormal)
}

func (w *scopeWorker) drainPollingModeCh() {
	select {
	case <-w.scope.pollingModeCh:
	default:
	}
}

// Both scope refresh and decision sending has to be done synchronously
// to avoid race conditions between CA decisions and capacity calculation in
// Flex Advisor in GCE. If scope refresh and decision notification happens asynchronously,
// CA cannot determine if the data returned by the Flex Advisor
// properly accounted the capacity needed for recent provisioning decision.
// GCE guarantees that if API call for decision notification finished it will account,
// that capacity change for the subsequent scope refreshes.
func (w *scopeWorker) refreshScope(ctx context.Context) {
	defer func() {
		w.scope.mutex.Lock()
		defer w.scope.mutex.Unlock()
		// lastRefreshAttemptAt should be set here instead of just in finishRefresh, because if FlexAdvisorProcessing is disabled and we don't update lastRefreshAttemptAt our refresh routine will take 100% of cpu
		// just entering refreshScope and immediately returning
		w.scope.lastRefreshAttemptAt = w.clock.Now()
	}()
	if !IsFlexAdvisorProcessingEnabled(w.experimentsManager) {
		klog.Infof("FlexAdvisor[async-worker]: loop processing is disabled by FlexAdvisorProcessing experiment, background worker returning early for flexibilityScopeKey=%v", w.scope.flexibilityScopeKey)
		return
	}
	instancesBeingProvisioned := w.scope.copyInstancesBeingProvisioned()

	generated, errs := w.instanceConfigGenerator.generateInstanceConfigs(w.scope.flexibilityScopeKey)

	if len(errs) > 0 {
		klog.Warningf("FlexAdvisor[async-worker]: %d error(s) observed while generating instance configurations, flexibilityScopeKey=%v, errs=%v ", len(errs), w.scope.flexibilityScopeKey, errs)
	}

	if generated == nil || len(generated.Configs) == 0 {
		errorMsg := fmt.Sprintf("generated 0 instance configurations, not calling GCE Flex Advisor, flexibilityScopeKey=%v", w.scope.flexibilityScopeKey)
		klog.Warningf("FlexAdvisor[async-worker]: %v", errorMsg)
		w.scope.finishRefresh(nil, instancesBeingProvisioned, nil, nil, w.clock.Now(), errors.New(errorMsg))
		return
	}

	// performs the actual API call and updates the scope.
	results, err := w.adviceProvider.FetchCapacityGuidance(ctx, w.scope.flexibilityScopeKey, generated.Configs)
	if err != nil {
		klog.Errorf("FlexAdvisor[async-worker]: Error refreshing cache, flexibilityScopeKey=%v, err=%v", w.scope.flexibilityScopeKey, err)
	}

	if err == nil {
		// validate the response from Flex Advisor and report issues (logs and metrics)
		w.validateResponse(generated.Configs, results)
	}

	for _, config := range results {
		config.SetDecisionChan(w.provisioningDecisionChan)
		config.SetProvider(w.scope.provider)
	}

	w.scope.finishRefresh(results, instancesBeingProvisioned, generated.GeneratedUsing, generated.CappedKeysMap, w.clock.Now(), err)
	if err == nil {
		w.updateRuleFilteringConditions(results)
	}
}

func (w *scopeWorker) validateResponse(generatedConfigs map[string]*api.InstanceConfig, results map[string]*api.InstanceAvailability) {
	missingConfigQuota := logging.FlexAdvisorResponseErrorsLoggingQuota()
	missingZoneQuota := logging.FlexAdvisorResponseErrorsLoggingQuota()
	negativeCountQuota := logging.FlexAdvisorResponseErrorsLoggingQuota()
	invalidScoreQuota := logging.FlexAdvisorResponseErrorsLoggingQuota()

	hasMissingZone := false
	hasNegativeCount := false
	hasInvalidScore := false

	for instanceConfigKey, instanceConfig := range generatedConfigs {
		availability, found := results[instanceConfigKey]
		if !found {
			klogx.V(4).UpTo(missingConfigQuota).Warningf("FlexAdvisor[async-worker]: Backend response missing requested instance configuration %q for flexibilityScopeKey %q", instanceConfigKey, w.scope.flexibilityScopeKey)
			metrics.Metrics.RegisterFlexAdvisorResponseError(metrics.ResponseMissingInstanceConfig)
			continue
		}

		snapshot := availability.NewSnapshot()

		for _, zone := range instanceConfig.Zones().UnsortedList() {
			count, found := snapshot.MaxAvailableInstances(zone)
			if !found {
				klogx.V(4).UpTo(missingZoneQuota).Warningf("FlexAdvisor[async-worker]: Backend response missing requested zone %q for instance configuration %q and flexibilityScopeKey %q", zone, instanceConfigKey, w.scope.flexibilityScopeKey)
				hasMissingZone = true
			} else {
				if count < 0 {
					klogx.V(4).UpTo(negativeCountQuota).Warningf("FlexAdvisor[async-worker]: Backend response has negative instance count %v for zone %q, instance configuration %q and flexibilityScopeKey %q: %v", count, zone, instanceConfigKey, w.scope.flexibilityScopeKey, snapshot)
					hasNegativeCount = true
				}
			}

			score, found := snapshot.GcePreferenceScore(zone)
			if !found {
				klogx.V(4).UpTo(invalidScoreQuota).Warningf("FlexAdvisor[async-worker]: Backend response missing GCE preference score for zone %q, instance configuration %q and flexibilityScopeKey %q: %v", zone, instanceConfigKey, w.scope.flexibilityScopeKey, snapshot)
			}
			if score < 0.0 || score > 1.0 {
				klogx.V(4).UpTo(invalidScoreQuota).Warningf("FlexAdvisor[async-worker]: Backend response has invalid preference score %v for zone %q, instance configuration %q and flexibilityScopeKey %q: %v", score, zone, instanceConfigKey, w.scope.flexibilityScopeKey, snapshot)
				hasInvalidScore = true
			}
		}
	}

	klogx.V(4).Over(missingConfigQuota).Warningf("FlexAdvisor[async-worker]: There were also %v other backend response missing requested instance configuration warnings for flexibilityScopeKey %q", -missingConfigQuota.Left(), w.scope.flexibilityScopeKey)
	klogx.V(4).Over(missingZoneQuota).Warningf("FlexAdvisor[async-worker]: There were also %v other backend response missing requested zone warnings for flexibilityScopeKey %q", -missingZoneQuota.Left(), w.scope.flexibilityScopeKey)
	klogx.V(4).Over(negativeCountQuota).Warningf("FlexAdvisor[async-worker]: There were also %v other backend response negative instance count warnings for flexibilityScopeKey %q", -negativeCountQuota.Left(), w.scope.flexibilityScopeKey)
	klogx.V(4).Over(invalidScoreQuota).Warningf("FlexAdvisor[async-worker]: There were also %v other backend response invalid preference score warnings for flexibilityScopeKey %q", -invalidScoreQuota.Left(), w.scope.flexibilityScopeKey)

	if hasNegativeCount {
		metrics.Metrics.RegisterFlexAdvisorResponseError(metrics.InvalidInstanceCount)
	}

	if hasInvalidScore {
		metrics.Metrics.RegisterFlexAdvisorResponseError(metrics.InvalidPreferenceScore)
	}

	if hasMissingZone {
		metrics.Metrics.RegisterFlexAdvisorResponseError(metrics.ResponseMissingZone)
	}
}

func (w *scopeWorker) sendProvisioningDecision(ctx context.Context, decision api.ProvisioningDecisionNotification) {
	err := w.adviceProvider.SendCapacityDecision(ctx, decision)
	metrics.Metrics.IncrementFlexAdvisorFeedbackDecisionCount(metrics.FADecisionFeedbackSent)
	if err != nil {
		metrics.Metrics.IncrementFlexAdvisorFeedbackDecisionCount(metrics.FADecisionFeedbackError)
		klog.Errorf("FlexAdvisor[async-worker]: Error when sending provision decision for flexibilityScopeKey=%v, instanceConfiguration=%v, err=%v", decision.FlexibilityScopeKey(), decision.InstanceConfigKey(), err)
	}
}

func newFlexibilityScope(provider instanceavailability.Provider, flexibilityScopeKey string, experimentsManager experiments.Manager, cancelFunc context.CancelFunc) *flexibilityScope {
	pollingMode := PollingModeNormal
	if IsFlexAdvisorAdaptivePollingEnabled(experimentsManager) {
		pollingMode = PollingModeSlow
	}
	scope := &flexibilityScope{
		provider:            provider,
		flexibilityScopeKey: flexibilityScopeKey,
		experimentsManager:  experimentsManager,
		instanceConfigs:     make(map[string]*api.InstanceAvailability),
		pollingModeCh:       make(chan struct{}, 1),
		cancelFunc:          cancelFunc,
	}
	scope.pollingMode.Store(pollingMode)
	scope.firstFetchWG.Add(1)
	return scope
}

// setPollingMode sets pollingMode and issues message to a channel if pollingMode has changed
func (s *flexibilityScope) setPollingMode(pollingMode PollingMode) {
	if prevMode := s.pollingMode.Swap(pollingMode); prevMode != pollingMode {
		klog.Infof("FlexAdvisor: changed pollingMode %v to %v for flexibilityScopeKey=%v", prevMode, pollingMode, s.flexibilityScopeKey)
		select {
		case s.pollingModeCh <- struct{}{}:
		default:
		}
	}
}

func (s *flexibilityScope) getPollingMode() PollingMode {
	return s.pollingMode.Load()
}

func (s *flexibilityScope) getAvailabilityData(instanceConfigKey string) *api.InstanceAvailability {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.instanceConfigs[instanceConfigKey]
}

// inFlightProvisions tracks instances being provisioned, organized by their instance configuration key and zone.
type inFlightProvisions struct {
	byInstanceConfig map[string]api.ZonalProvisionCounts
}

func (s *flexibilityScope) copyInstancesBeingProvisioned() *inFlightProvisions {
	s.mutex.RLock()
	configsToProcess := make([]*api.InstanceAvailability, 0, len(s.instanceConfigs))
	for _, config := range s.instanceConfigs {
		configsToProcess = append(configsToProcess, config)
	}
	s.mutex.RUnlock()

	inflight := &inFlightProvisions{
		byInstanceConfig: make(map[string]api.ZonalProvisionCounts),
	}
	for _, config := range configsToProcess {
		inflight.byInstanceConfig[config.InstanceConfigKey()] = config.ZonalProvisionsSinceLastRefresh()
	}
	return inflight
}

func (s *flexibilityScope) finishRefresh(newConfigs map[string]*api.InstanceAvailability, instancesBeingProvisioned *inFlightProvisions, generatedUsing crd.CRD, cappedKeysMap map[string]bool, refreshedAt time.Time, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastErr = err

	if err == nil {
		// Only update data on success
		for key, newConfig := range newConfigs {
			if oldConfig := s.instanceConfigs[key]; oldConfig != nil {
				newConfig.ReconcileAndUpdate(oldConfig, instancesBeingProvisioned.byInstanceConfig[key])
			}
		}
		s.generatedUsing = generatedUsing
		s.cappedKeysMap = cappedKeysMap
		s.instanceConfigs = newConfigs
		s.lastSuccessfulRefreshAt = refreshedAt
	}
}

// fetchStats holds a point-in-time snapshot of a scope's most recent refresh error and last successful refresh timestamp.
type fetchStats struct {
	lastFetchError          error
	lastSuccessfulRefreshAt time.Time
}

// getFetchStats returns the scope's last refresh error and last successful refresh timestamp under a single read lock.
func (s *flexibilityScope) getFetchStats() fetchStats {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fetchStats{
		s.lastErr,
		s.lastSuccessfulRefreshAt,
	}
}

func (s *flexibilityScope) getLastRefreshAttemptAt() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastRefreshAttemptAt
}

// getKeyCapState returns instanceConfigKey generation cap state:
// false - was generated, not capped
// true - was generated, was capped
// no value - was not generated
func (s *flexibilityScope) getKeyCapState(instanceConfigKey string) (bool, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, ok := s.cappedKeysMap[instanceConfigKey]
	return val, ok
}

func (s *flexibilityScope) getGeneratedUsing() crd.CRD {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.generatedUsing
}
