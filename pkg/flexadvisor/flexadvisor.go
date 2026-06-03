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
	oss_metrics "k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
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
	defaultMaxInstanceConfigs = 200
	firstFetchTimeout         = 15 * time.Second
)

type flexAdvisorMetrics interface {
	IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, isScaleUpAnyway *bool, keyGenerationState metrics.KeyGenerationState)
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
}

type option func(*flexAdvisor)

// NewFlexAdvisor creates a new flexAdvisor object.
func NewFlexAdvisor(ctx context.Context, adviceProvider api.AdviceProvider, cccLister lister.Lister, instanceConfigCloudProvider instanceConfigCloudProvider, optionsTracker *optstracking.OptionsTracker, opts ...option) (*flexAdvisor, error) {
	if adviceProvider == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil flexadvisor.AdviceProvider")
	}
	if cccLister == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil lister.lister")
	}
	if instanceConfigCloudProvider == nil {
		return nil, fmt.Errorf("flex advisor expects a non nil flexadvisor.instanceConfigCloudProvider")
	}

	ig := NewInstanceConfigGenerator(cccLister, instanceConfigCloudProvider, optionsTracker)

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
	}
	for _, opt := range opts {
		opt(f)
	}

	go wait.BackoffUntil(f.removeExpiredFlexibilityScopes, &backOffManager{f.clock, refreshInterval, nil}, true, f.context.Done())
	return f, nil
}

// GetInstanceAvailability tries to get InstanceAvailability from cache. Unlike AwaitInstanceAvailability if cache is not available, it does not wait for background job to fetch it.
// WARNING: THIS METHOD DOES NOT UPDATE KEEP-ALIVE. This method is meant to be as non-blocking as possible, acquiring only RLocks in happy path. See b/514258103 for more
func (f *flexAdvisor) GetInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) *instanceavailability.Snapshot {
	scope, scopeFound := f.getScope(flexibilityScopeKey)
	if !scopeFound || scope == nil {
		scope = f.addFlexibilityScopeIfNotExist(flexibilityScopeKey)
	}
	config := scope.getInstanceConfig(instanceConfigKey)
	if config == nil {
		return nil
	}
	return config.NewSnapshot()
}

func (f *flexAdvisor) RegisterFlexibilityScope(flexibilityScopeKey string) {
	f.addFlexibilityScopeIfNotExist(flexibilityScopeKey)
}

func (f *flexAdvisor) AwaitInstanceAvailability(flexibilityScopeKey, instanceConfigKey string) (*instanceavailability.Snapshot, error) {
	waitStart := time.Now()
	var err error
	defer func() {
		waitDuration := time.Since(waitStart)
		oss_metrics.UpdateDuration("FlexAdvisor:AwaitInstanceAvailability", waitDuration)
		if waitDuration > 100*time.Millisecond {
			if err == nil {
				klog.Warningf("FlexAdvisor: waited %v for GCE Flex Advisor consultation, flexibilityScopeKey=%v, err=nil", waitDuration, flexibilityScopeKey)
			} else {
				klog.Errorf("FlexAdvisor: waited %v for GCE Flex Advisor consultation, flexibilityScopeKey=%v, err=%v", waitDuration, flexibilityScopeKey, err)
			}
		}
	}()
	scope := f.addFlexibilityScopeIfNotExist(flexibilityScopeKey)
	err = f.waitForFirstFetchOrTimeOut(scope)
	if err != nil {
		return nil, err
	}

	value := scope.getInstanceConfig(instanceConfigKey)
	if value == nil {
		lastErr := scope.getLastErr()
		err = fmt.Errorf("instanceConfigKey=%v not present in availability data after refresh, flexibilityScopeKey=%v, lastRefreshErr=%v, cccUsesScaleUpAnyway=%v, keyGenerationState=%v", instanceConfigKey, flexibilityScopeKey, lastErr, formatBoolPtr(f.isScaleUpAnyway(scope)), f.keyGenerationState(scope, instanceConfigKey))
		if lastErr != nil {
			f.IncrementFlexAdvisorCacheQueryCount(metrics.FACacheMissFetchFailed, flexibilityScopeKey, instanceConfigKey)
		} else {
			f.IncrementFlexAdvisorCacheQueryCount(metrics.FACacheMissNoInstanceConfigKey, flexibilityScopeKey, instanceConfigKey)
		}
		return nil, err
	}
	return value.NewSnapshot(), nil
}

func (f *flexAdvisor) waitForFirstFetchOrTimeOut(scope *flexibilityScope) error {
	firstFetchDone := make(chan struct{})
	go func() {
		scope.firstFetchWG.Wait()
		close(firstFetchDone)
	}()
	timeout := f.calculateFirstFetchTimeout()
	timer := f.clock.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-firstFetchDone:
		return nil
	case <-timer.C():
		return fmt.Errorf("timeout waiting for GCE Flex Advisor consultation, flexibilityScopeKey=%v", scope.flexibilityScopeKey)
	}
}

func (f *flexAdvisor) calculateFirstFetchTimeout() time.Duration {
	if f.optionsTracker == nil {
		return firstFetchTimeout
	}
	durationString := f.optionsTracker.ExperimentsManager().EvaluateStringFlagOrFailsafe(
		experiments.FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag,
		// We could fmt.Sprintf here firstFetchTimeout value, we explicitly handle default value lower in case there was bug in further processing of experiment value
		"",
	)
	if durationString == "" {
		return firstFetchTimeout
	}
	parsedSeconds, err := strconv.ParseInt(durationString, 10, 64)
	if err != nil || parsedSeconds <= 0 {
		klog.Errorf("FlexAdvisor: invalid value for flag %s: %q, defaulting to %v. error: %v",
			experiments.FlexAdvisorAwaitInstanceAvailabilityTimeoutSecondsFlag,
			durationString,
			firstFetchTimeout,
			err,
		)
		return firstFetchTimeout
	}
	return time.Duration(parsedSeconds) * time.Second
}

// IncrementFlexAdvisorCacheQueryCount gathers additional debugging info and calls underlying prometheus' IncrementFlexAdvisorCacheQueryCount
func (f *flexAdvisor) IncrementFlexAdvisorCacheQueryCount(result metrics.FACacheQueryResult, flexibilityScopeKey string, instanceConfigKey string) {
	scope, _ := f.getScope(flexibilityScopeKey)

	isScaleUpAnyway := f.isScaleUpAnyway(scope)
	state := f.keyGenerationState(scope, instanceConfigKey)

	f.metrics.IncrementFlexAdvisorCacheQueryCount(result, isScaleUpAnyway, state)
}

// isScaleUpAnyway returns true/false based on CCC ScaleUpAnyway field, nil if cannot determine
func (f *flexAdvisor) isScaleUpAnyway(scope *flexibilityScope) *bool {
	if scope == nil {
		return nil
	}
	crd, err := f.cccLister.GetCrd(scope.flexibilityScopeKey)
	if err != nil {
		klog.Errorf("FlexAdvisor: error getting crd flexibilityScopeKey=%v, err=%v", scope.flexibilityScopeKey, err)
		return nil
	}
	return ptr.To(crd.ScaleUpAnyway())
}

func formatBoolPtr(b *bool) string {
	if b == nil {
		return "nil"
	}
	return strconv.FormatBool(*b)
}

// keyGenerationState returns KeyGenerationState val for emitting flexadvisor_cache_query_count
func (f *flexAdvisor) keyGenerationState(scope *flexibilityScope, instanceConfigKey string) metrics.KeyGenerationState {
	if scope == nil {
		return ""
	}
	val, ok := scope.cappedKeysMap[instanceConfigKey]
	if !ok {
		return metrics.KeyGenerationStateNotGenerated
	}
	if val {
		return metrics.KeyGenerationStateGeneratedButCapped
	}
	return metrics.KeyGenerationStateGeneratedAndSent
}

func (f *flexAdvisor) MarkUsed(flexibilityScopeKey, instanceConfigKey, guidanceId, decisionId string, zonalInstancesToProvision map[string]int) error {
	scope, scopeFound := f.getScope(flexibilityScopeKey)
	if !scopeFound || scope == nil {
		return fmt.Errorf("flexibility scope not found for key: %s", flexibilityScopeKey)
	}
	config := scope.getInstanceConfig(instanceConfigKey)
	if config == nil {
		return fmt.Errorf("instance configuration not found for flexibilityScopeKey: %s, instanceConfigurationKey: %s", flexibilityScopeKey, instanceConfigKey)
	}
	config.MarkUsed(zonalInstancesToProvision, decisionId, guidanceId)
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

func (f *flexAdvisor) getScope(flexibilityScopeKey string) (*flexibilityScope, bool) {
	f.rwMutex.RLock()
	defer f.rwMutex.RUnlock()
	scope, scopeFound := f.scopes[flexibilityScopeKey]
	return scope, scopeFound
}

func (f *flexAdvisor) addFlexibilityScopeIfNotExist(flexibilityScopeKey string) *flexibilityScope {
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()

	scope, found := f.scopes[flexibilityScopeKey]
	if found {
		f.scopesActiveUntil[flexibilityScopeKey] = f.clock.Now().Add(keepAliveInterval)
		return scope
	}
	ctx, cancel := context.WithCancel(f.context)
	scope = newFlexibilityScope(f, flexibilityScopeKey, cancel)

	f.scopes[flexibilityScopeKey] = scope
	f.scopesActiveUntil[flexibilityScopeKey] = f.clock.Now().Add(keepAliveInterval)

	metrics.Metrics.UpdateFlexAdvisorActiveScopes(len(f.scopes))

	worker := newScopeWorker(scope, f)

	go worker.run(ctx)
	klog.V(4).Infof("FlexAdvisor: Registered a new flexibility scope: %s", flexibilityScopeKey)

	return scope
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
