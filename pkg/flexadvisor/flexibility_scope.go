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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const provisioningDecisionNotificationChanSize = 10

type flexibilityScope struct {
	provider            instanceavailability.Provider
	flexibilityScopeKey string
	instanceConfigs     map[string]*api.InstanceAvailability
	mutex               sync.Mutex
	firstFetchWG        sync.WaitGroup
	cancelFunc          context.CancelFunc
	lastErr             error
	// cappedKeysMap contains all generated keys:
	// - If a key was capped locally (not sent to the backend), its value is true.
	// - If a key was sent to the backend, its value is false.
	// Keys that were not generated at all (filtered out by rules) are not in this map.
	// Keys that were sent (value is false) but are missing from instanceConfigs were dropped by the backend.
	cappedKeysMap map[string]bool
}

type scopeWorker struct {
	scope                    *flexibilityScope
	provisioningDecisionChan chan api.ProvisioningDecisionNotification
	adviceProvider           api.AdviceProvider
	clock                    clock.Clock
	instanceConfigGenerator  *instanceConfigGenerator
}

func newScopeWorker(scope *flexibilityScope, f *flexAdvisor) *scopeWorker {
	return &scopeWorker{
		scope:                    scope,
		provisioningDecisionChan: make(chan api.ProvisioningDecisionNotification, provisioningDecisionNotificationChanSize),
		adviceProvider:           f.adviceProvider,
		clock:                    f.clock,
		instanceConfigGenerator:  f.instanceConfigGenerator,
	}
}

func (w *scopeWorker) run(ctx context.Context) {
	w.refreshScope(ctx)
	w.scope.firstFetchWG.Done()
	for {
		select {
		case decision := <-w.provisioningDecisionChan:
			w.sendProvisioningDecision(ctx, decision)
		case <-w.clock.After(refreshInterval):
			w.refreshScope(ctx)
		case <-ctx.Done():
			return
		}
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
	instancesBeingProvisioned := w.scope.copyInstancesBeingProvisioned()

	generated, errs := w.instanceConfigGenerator.generateInstanceConfigs(w.scope.flexibilityScopeKey)

	if len(errs) > 0 {
		klog.Warningf("FlexAdvisor[async-worker]: %d error(s) observed while generating instance configurations, flexibilityScopeKey=%v, errs=%v ", len(errs), w.scope.flexibilityScopeKey, errs)
	}

	if generated == nil || len(generated.Configs) == 0 {
		errorMsg := fmt.Sprintf("generated 0 instance configurations, not calling GCE Flex Advisor, flexibilityScopeKey=%v", w.scope.flexibilityScopeKey)
		klog.Warningf("FlexAdvisor[async-worker]: %v", errorMsg)
		w.scope.finishRefresh(nil, instancesBeingProvisioned, nil, errors.New(errorMsg))
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

	w.scope.finishRefresh(results, instancesBeingProvisioned, generated.CappedKeysMap, err)
}

func (w *scopeWorker) validateResponse(generatedConfigs map[string]*api.InstanceConfig, results map[string]*api.InstanceAvailability) {
	hasMissingZone := false
	hasNegativeCount := false
	hasInvalidScore := false

	for instanceConfigKey, instanceConfig := range generatedConfigs {
		availability, found := results[instanceConfigKey]
		if !found {
			klog.Warningf("FlexAdvisor[async-worker]: Backend response missing requested instance configuration %q for flexibilityScopeKey %q", instanceConfigKey, w.scope.flexibilityScopeKey)
			metrics.Metrics.RegisterFlexAdvisorResponseError(metrics.ResponseMissingInstanceConfig)
			continue
		}

		snapshot := availability.NewSnapshot()

		for _, zone := range instanceConfig.Zones().UnsortedList() {
			count, found := snapshot.MaxAvailableInstances(zone)
			if !found {
				klog.Warningf("FlexAdvisor[async-worker]: Backend response missing requested zone %q for instance configuration %q and flexibilityScopeKey %q", zone, instanceConfigKey, w.scope.flexibilityScopeKey)
				hasMissingZone = true
			} else {
				if count < 0 {
					klog.Warningf("FlexAdvisor[async-worker]: Backend response has negative instance count %d for zone %q, instance configuration %q and flexibilityScopeKey %q: %v", count, zone, instanceConfigKey, w.scope.flexibilityScopeKey, snapshot)
					hasNegativeCount = true
				}
			}

			score := snapshot.GcePreferenceScore(zone)
			if score < 0.0 || score > 1.0 {
				klog.Warningf("FlexAdvisor[async-worker]: Backend response has invalid preference score %f for zone %q, instance configuration %q and flexibilityScopeKey %q: %v", score, zone, instanceConfigKey, w.scope.flexibilityScopeKey, snapshot)
				hasInvalidScore = true
			}
		}
	}

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

func newFlexibilityScope(provider instanceavailability.Provider, flexibilityScopeKey string, cancelFunc context.CancelFunc) *flexibilityScope {
	scope := &flexibilityScope{
		provider:            provider,
		flexibilityScopeKey: flexibilityScopeKey,
		instanceConfigs:     make(map[string]*api.InstanceAvailability),
		cancelFunc:          cancelFunc,
	}
	scope.firstFetchWG.Add(1)
	return scope
}

func (s *flexibilityScope) getInstanceConfig(instanceConfigKey string) *api.InstanceAvailability {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.instanceConfigs[instanceConfigKey]
}

// inFlightProvisions tracks instances being provisioned, organized by their instance configuration key and zone.
type inFlightProvisions struct {
	byInstanceConfig map[string]api.ZonalProvisionCounts
}

func (s *flexibilityScope) copyInstancesBeingProvisioned() *inFlightProvisions {
	s.mutex.Lock()
	configsToProcess := make([]*api.InstanceAvailability, 0, len(s.instanceConfigs))
	for _, config := range s.instanceConfigs {
		configsToProcess = append(configsToProcess, config)
	}
	s.mutex.Unlock()

	inflight := &inFlightProvisions{
		byInstanceConfig: make(map[string]api.ZonalProvisionCounts),
	}
	for _, config := range configsToProcess {
		inflight.byInstanceConfig[config.InstanceConfigKey()] = config.ZonalProvisionsSinceLastRefresh()
	}
	return inflight
}

func (s *flexibilityScope) finishRefresh(newConfigs map[string]*api.InstanceAvailability, instancesBeingProvisioned *inFlightProvisions, cappedKeysMap map[string]bool, err error) {
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
		s.cappedKeysMap = cappedKeysMap
		s.instanceConfigs = newConfigs
	}
}

func (s *flexibilityScope) getLastErr() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.lastErr
}
